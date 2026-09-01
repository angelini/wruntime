use std::collections::{BTreeMap, HashMap};
use std::fmt;

use http::header::{HeaderMap, CONTENT_ENCODING, CONTENT_TYPE};
use mime::{CHARSET, UTF_8};
use prost::Message as _;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor};
use serde_json::{Map, Number, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestRepresentation {
    Protobuf,
    Json,
    Form,
}

#[derive(Debug)]
pub(crate) struct MediaTypeError;

#[derive(Debug)]
pub(crate) struct TranscodeError {
    detail: String,
}

impl TranscodeError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    fn field(field: &FieldDescriptor, reason: &str) -> Self {
        Self::new(format!("field '{}': {reason}", field.name()))
    }
}

impl fmt::Display for TranscodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for TranscodeError {}

pub(crate) fn select_request_representation(
    headers: &HeaderMap,
    body_is_empty: bool,
) -> Result<RequestRepresentation, MediaTypeError> {
    let mut encodings = headers.get_all(CONTENT_ENCODING).iter();
    if let Some(encoding) = encodings.next() {
        if encodings.next().is_some()
            || !encoding
                .to_str()
                .is_ok_and(|encoding| encoding.trim().eq_ignore_ascii_case("identity"))
        {
            return Err(MediaTypeError);
        }
    }

    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return body_is_empty
            .then_some(RequestRepresentation::Protobuf)
            .ok_or(MediaTypeError);
    };
    if values.next().is_some() {
        return Err(MediaTypeError);
    }

    let media_type = value
        .to_str()
        .map_err(|_| MediaTypeError)?
        .parse::<mime::Mime>()
        .map_err(|_| MediaTypeError)?;

    let representation = match media_type.essence_str() {
        "application/x-protobuf" => RequestRepresentation::Protobuf,
        "application/json" => RequestRepresentation::Json,
        "application/x-www-form-urlencoded" => RequestRepresentation::Form,
        _ => return Err(MediaTypeError),
    };

    if matches!(
        representation,
        RequestRepresentation::Json | RequestRepresentation::Form
    ) && media_type
        .params()
        .any(|(name, value)| name == CHARSET && value != UTF_8)
    {
        return Err(MediaTypeError);
    }

    Ok(representation)
}

pub(crate) fn transcode_request(
    descriptor: MessageDescriptor,
    representation: RequestRepresentation,
    body: &[u8],
) -> Result<Vec<u8>, TranscodeError> {
    let message = match representation {
        RequestRepresentation::Protobuf => DynamicMessage::decode(descriptor, body)
            .map_err(|_| TranscodeError::new("invalid protobuf wire body"))?,
        RequestRepresentation::Json => decode_json(descriptor, body)?,
        RequestRepresentation::Form => decode_form(descriptor, body)?,
    };
    Ok(message.encode_to_vec())
}

fn decode_json(
    descriptor: MessageDescriptor,
    body: &[u8],
) -> Result<DynamicMessage, TranscodeError> {
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let message = DynamicMessage::deserialize(descriptor, &mut deserializer)
        .map_err(|_| TranscodeError::new("invalid protobuf JSON body"))?;
    deserializer
        .end()
        .map_err(|_| TranscodeError::new("trailing JSON input"))?;
    Ok(message)
}

fn decode_form(
    descriptor: MessageDescriptor,
    body: &[u8],
) -> Result<DynamicMessage, TranscodeError> {
    let mut submitted = BTreeMap::<u32, SubmittedField>::new();
    let mut submitted_oneofs = HashMap::<String, u32>::new();

    for (key, value) in parse_form(body)? {
        if key.contains(['.', '[', ']']) {
            return Err(TranscodeError::new(
                "nested or bracketed form field names are unsupported",
            ));
        }

        let field = resolve_form_field(&descriptor, &key)?;
        if field.is_map() || field.is_group() || matches!(field.kind(), Kind::Message(_)) {
            return Err(TranscodeError::field(
                &field,
                "message, group, and map form fields are unsupported",
            ));
        }

        if let Some(oneof) = field.containing_oneof() {
            match submitted_oneofs.get(oneof.full_name()) {
                Some(number) if *number != field.number() => {
                    return Err(TranscodeError::new(format!(
                        "oneof '{}': multiple members were submitted",
                        oneof.name()
                    )));
                }
                Some(_) => {}
                None => {
                    submitted_oneofs.insert(oneof.full_name().to_owned(), field.number());
                }
            }
        }

        let json_value = form_scalar_to_json(&field, &value)?;
        match submitted.entry(field.number()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(SubmittedField {
                    field,
                    values: vec![json_value],
                });
            }
            std::collections::btree_map::Entry::Occupied(mut entry) if field.is_list() => {
                entry.get_mut().values.push(json_value);
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(TranscodeError::field(
                    &field,
                    "duplicate singular form field",
                ));
            }
        }
    }

    let mut object = Map::new();
    for SubmittedField { field, mut values } in submitted.into_values() {
        let value = if field.is_list() {
            Value::Array(values)
        } else {
            values.pop().expect("submitted fields always have a value")
        };
        object.insert(field.json_name().to_owned(), value);
    }

    let json = serde_json::to_vec(&Value::Object(object))
        .map_err(|_| TranscodeError::new("failed to normalize form body"))?;
    decode_json(descriptor, &json)
        .map_err(|_| TranscodeError::new("form body is incompatible with the protobuf schema"))
}

struct SubmittedField {
    field: FieldDescriptor,
    values: Vec<Value>,
}

fn resolve_form_field(
    descriptor: &MessageDescriptor,
    key: &str,
) -> Result<FieldDescriptor, TranscodeError> {
    let by_name = descriptor.get_field_by_name(key);
    let by_json_name = descriptor.get_field_by_json_name(key);
    match (by_name, by_json_name) {
        (Some(by_name), Some(by_json_name)) if by_name.number() != by_json_name.number() => Err(
            TranscodeError::new(format!("form field '{key}' is ambiguous")),
        ),
        (Some(field), _) | (_, Some(field)) => Ok(field),
        (None, None) => Err(TranscodeError::new(format!("unknown form field '{key}'"))),
    }
}

fn form_scalar_to_json(field: &FieldDescriptor, value: &str) -> Result<Value, TranscodeError> {
    let invalid = || TranscodeError::field(field, "invalid scalar value");
    match field.kind() {
        Kind::String | Kind::Bytes => Ok(Value::String(value.to_owned())),
        Kind::Bool => match value {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => Err(invalid()),
        },
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => value
            .parse::<i32>()
            .map(Number::from)
            .map(Value::Number)
            .map_err(|_| invalid()),
        Kind::Uint32 | Kind::Fixed32 => value
            .parse::<u32>()
            .map(Number::from)
            .map(Value::Number)
            .map_err(|_| invalid()),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => value
            .parse::<i64>()
            .map(|value| Value::String(value.to_string()))
            .map_err(|_| invalid()),
        Kind::Uint64 | Kind::Fixed64 => value
            .parse::<u64>()
            .map(|value| Value::String(value.to_string()))
            .map_err(|_| invalid()),
        Kind::Float => float_to_json(value, true).map_err(|_| invalid()),
        Kind::Double => float_to_json(value, false).map_err(|_| invalid()),
        Kind::Enum(descriptor) => {
            if descriptor.get_value_by_name(value).is_some() {
                Ok(Value::String(value.to_owned()))
            } else {
                value
                    .parse::<i32>()
                    .map(Number::from)
                    .map(Value::Number)
                    .map_err(|_| invalid())
            }
        }
        Kind::Message(_) => Err(TranscodeError::field(
            field,
            "message form fields are unsupported",
        )),
    }
}

fn float_to_json(value: &str, single_precision: bool) -> Result<Value, ()> {
    if matches!(value, "NaN" | "Infinity" | "-Infinity") {
        return Ok(Value::String(value.to_owned()));
    }

    let value = if single_precision {
        let parsed = value.parse::<f32>().map_err(|_| ())?;
        if !parsed.is_finite() {
            return Err(());
        }
        f64::from(parsed)
    } else {
        let parsed = value.parse::<f64>().map_err(|_| ())?;
        if !parsed.is_finite() {
            return Err(());
        }
        parsed
    };
    Number::from_f64(value).map(Value::Number).ok_or(())
}

fn parse_form(body: &[u8]) -> Result<Vec<(String, String)>, TranscodeError> {
    let mut pairs = Vec::new();
    for pair in body.split(|byte| *byte == b'&') {
        if pair.is_empty() {
            continue;
        }
        let mut parts = pair.splitn(2, |byte| *byte == b'=');
        let key = decode_form_component(parts.next().unwrap_or_default())?;
        let value = decode_form_component(parts.next().unwrap_or_default())?;
        pairs.push((key, value));
    }
    Ok(pairs)
}

fn decode_form_component(encoded: &[u8]) -> Result<String, TranscodeError> {
    let mut decoded = Vec::with_capacity(encoded.len());
    let mut index = 0;
    while index < encoded.len() {
        match encoded[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' => {
                let high = encoded.get(index + 1).and_then(|byte| hex_value(*byte));
                let low = encoded.get(index + 2).and_then(|byte| hex_value(*byte));
                let (Some(high), Some(low)) = (high, low) else {
                    return Err(TranscodeError::new("malformed form percent encoding"));
                };
                decoded.push((high << 4) | low);
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).map_err(|_| TranscodeError::new("form body is not valid UTF-8"))
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;
    use prost_reflect::{DescriptorPool, Value as ReflectValue};
    use prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
        FileDescriptorProto, FileDescriptorSet, MessageOptions, OneofDescriptorProto,
    };

    use super::*;

    fn field(name: &str, number: i32, kind: Type) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(number),
            label: Some(Label::Optional as i32),
            r#type: Some(kind as i32),
            json_name: Some(to_json_name(name)),
            ..Default::default()
        }
    }

    fn to_json_name(name: &str) -> String {
        let mut uppercase = false;
        name.chars()
            .filter_map(|character| {
                if character == '_' {
                    uppercase = true;
                    None
                } else if uppercase {
                    uppercase = false;
                    Some(character.to_ascii_uppercase())
                } else {
                    Some(character)
                }
            })
            .collect()
    }

    fn descriptor() -> MessageDescriptor {
        let mut state = field("state", 7, Type::Enum);
        state.type_name = Some(".test.State".into());

        let mut tags = field("tags", 8, Type::String);
        tags.label = Some(Label::Repeated as i32);

        let mut choice_text = field("choice_text", 9, Type::String);
        choice_text.oneof_index = Some(0);
        let mut choice_number = field("choice_number", 10, Type::Int32);
        choice_number.oneof_index = Some(0);

        let mut nested = field("nested", 11, Type::Message);
        nested.type_name = Some(".test.Nested".into());

        let map_entry = DescriptorProto {
            name: Some("LabelsEntry".into()),
            field: vec![
                field("key", 1, Type::String),
                field("value", 2, Type::String),
            ],
            options: Some(MessageOptions {
                map_entry: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut labels = field("labels", 12, Type::Message);
        labels.label = Some(Label::Repeated as i32);
        labels.type_name = Some(".test.Request.LabelsEntry".into());

        let request = DescriptorProto {
            name: Some("Request".into()),
            field: vec![
                field("display_name", 1, Type::String),
                field("enabled", 2, Type::Bool),
                field("count", 3, Type::Int32),
                field("big", 4, Type::Int64),
                field("ratio", 5, Type::Double),
                field("payload", 6, Type::Bytes),
                state,
                tags,
                choice_text,
                choice_number,
                nested,
                labels,
                field("unsigned", 13, Type::Uint64),
                field("single", 14, Type::Float),
            ],
            nested_type: vec![map_entry],
            oneof_decl: vec![OneofDescriptorProto {
                name: Some("choice".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let nested = DescriptorProto {
            name: Some("Nested".into()),
            field: vec![field("value", 1, Type::String)],
            ..Default::default()
        };
        let state = EnumDescriptorProto {
            name: Some("State".into()),
            value: vec![
                EnumValueDescriptorProto {
                    name: Some("STATE_UNSPECIFIED".into()),
                    number: Some(0),
                    ..Default::default()
                },
                EnumValueDescriptorProto {
                    name: Some("ACTIVE".into()),
                    number: Some(1),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let file = FileDescriptorProto {
            name: Some("transcoding.proto".into()),
            package: Some("test".into()),
            message_type: vec![request, nested],
            enum_type: vec![state],
            syntax: Some("proto3".into()),
            ..Default::default()
        };
        let pool = DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
            .expect("test descriptor must be valid");
        pool.get_message_by_name("test.Request").unwrap()
    }

    fn headers(content_type: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(content_type) = content_type {
            headers.insert(CONTENT_TYPE, HeaderValue::from_str(content_type).unwrap());
        }
        headers
    }

    #[test]
    fn selects_supported_media_types_and_empty_default() {
        assert_eq!(
            select_request_representation(&headers(None), true).unwrap(),
            RequestRepresentation::Protobuf
        );
        assert_eq!(
            select_request_representation(
                &headers(Some("application/json; charset=UTF-8; profile=public")),
                false
            )
            .unwrap(),
            RequestRepresentation::Json
        );
        assert_eq!(
            select_request_representation(
                &headers(Some("application/x-www-form-urlencoded; charset=utf-8")),
                false
            )
            .unwrap(),
            RequestRepresentation::Form
        );
        assert!(select_request_representation(&headers(None), false).is_err());
        assert!(select_request_representation(&headers(Some("text/plain")), true).is_err());
        assert!(select_request_representation(
            &headers(Some("application/json; charset=iso-8859-1")),
            false
        )
        .is_err());

        let mut encoded = headers(Some("application/json"));
        encoded.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        assert!(select_request_representation(&encoded, false).is_err());
    }

    #[test]
    fn decodes_canonical_json_and_rejects_unknown_or_trailing_input() {
        let descriptor = descriptor();
        let json = br#"{
            "displayName":"item",
            "enabled":true,
            "big":"9223372036854775807",
            "payload":"aGk=",
            "state":"ACTIVE",
            "tags":["a","b"],
            "nested":{"value":"inside"},
            "labels":{"region":"west"},
            "choiceText":"selected"
        }"#;
        let bytes =
            transcode_request(descriptor.clone(), RequestRepresentation::Json, json).unwrap();
        let decoded = DynamicMessage::decode(descriptor.clone(), bytes.as_slice()).unwrap();
        assert_eq!(
            decoded.get_field_by_name("display_name").unwrap().as_ref(),
            &ReflectValue::String("item".into())
        );
        assert!(transcode_request(
            descriptor.clone(),
            RequestRepresentation::Json,
            br#"{"unknown":1}"#
        )
        .is_err());
        assert!(
            transcode_request(descriptor.clone(), RequestRepresentation::Json, br#"{} {}"#)
                .is_err()
        );
        assert!(transcode_request(
            descriptor,
            RequestRepresentation::Json,
            br#"{"choiceText":"a","choiceNumber":1}"#
        )
        .is_err());
    }

    #[test]
    fn decodes_flat_form_scalars_repeated_aliases_and_oneof() {
        let descriptor = descriptor();
        let body = b"display_name=hello+world&enabled=true&count=-7&big=9223372036854775807&ratio=1.5&payload=aGk%3D&state=ACTIVE&tags=a&tags=b&choiceNumber=9&unsigned=18446744073709551615&single=2.5";
        let actual =
            transcode_request(descriptor.clone(), RequestRepresentation::Form, body).unwrap();
        let expected = transcode_request(
            descriptor,
            RequestRepresentation::Json,
            br#"{"displayName":"hello world","enabled":true,"count":-7,"big":"9223372036854775807","ratio":1.5,"payload":"aGk=","state":"ACTIVE","tags":["a","b"],"choiceNumber":9,"unsigned":"18446744073709551615","single":2.5}"#,
        )
        .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn rejects_form_duplicates_oneof_conflicts_and_unsupported_fields() {
        let descriptor = descriptor();
        for body in [
            b"display_name=a&displayName=b".as_slice(),
            b"choice_text=a&choiceNumber=1",
            b"nested=value",
            b"labels=value",
            b"unknown=value",
            b"display_name.first=value",
        ] {
            assert!(
                transcode_request(descriptor.clone(), RequestRepresentation::Form, body).is_err(),
                "body should be rejected"
            );
        }
    }

    #[test]
    fn rejects_malformed_form_encoding_and_invalid_scalar_values() {
        let descriptor = descriptor();
        for body in [
            b"display_name=%GG".as_slice(),
            b"display_name=%FF",
            b"enabled=yes",
            b"count=2147483648",
            b"payload=not-base64!",
            b"ratio=inf",
            b"state=missing",
        ] {
            assert!(
                transcode_request(descriptor.clone(), RequestRepresentation::Form, body).is_err(),
                "body should be rejected"
            );
        }
    }

    #[test]
    fn accepts_empty_protobuf_and_form_but_not_empty_json() {
        let descriptor = descriptor();
        assert!(
            transcode_request(descriptor.clone(), RequestRepresentation::Protobuf, b"").is_ok()
        );
        assert!(transcode_request(descriptor.clone(), RequestRepresentation::Form, b"").is_ok());
        assert!(transcode_request(descriptor, RequestRepresentation::Json, b"").is_err());
    }
}
