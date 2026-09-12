//! Parameterized generators for systemd units, Dockerfiles, and docker-compose files.
//!
//! Template variables like `{run_user}`, `{run_group}`, `{secret_key}` are emitted
//! as literal `{...}` strings and resolved later by `helpers::resolve_template()`.

/// Reviewed OCI index identity for distroless cc-debian13 `latest`.
///
/// Keep the digest in the generated deployment contract so rebuilding an immutable
/// wruntime bundle cannot silently select a different base image.
const DISTROLESS_CC_DEBIAN13: &str =
    "gcr.io/distroless/cc-debian13@sha256:9b615fff20e1a4fad29c2b30562580b212c7dd5e2225236735cca0070ed11c78";

/// Stable manager unit directory below runtime-mask precedence.
pub const MANAGER_SYSTEMD_UNIT_DIR: &str = "/usr/local/lib/systemd/system";
/// Stable manager unit installed outside `/etc` so a runtime mask overrides it.
pub const MANAGER_SYSTEMD_UNIT_PATH: &str = "/usr/local/lib/systemd/system/wr-manager.service";

/// A systemd service unit definition.
pub struct ServiceUnit<'a> {
    pub description: &'a str,
    pub binary_path: &'a str,
    pub config_path: &'a str,
    pub working_directory: &'a str,
    /// Extra `Environment=KEY=VALUE` lines. Values may contain template vars like `{secret_key}`.
    pub env_vars: Vec<(&'a str, &'a str)>,
    pub no_otel: bool,
    /// Extra `After=` dependencies (network.target is always included).
    pub after: Vec<&'a str>,
    /// `Wants=` dependencies. Unlike `Requires=`, these do not cascade a
    /// separately requested proxy stop into the engine unit.
    pub wants: Vec<&'a str>,
}

impl ServiceUnit<'_> {
    /// Render a systemd unit file. Always includes `{run_user}` and `{run_group}` template vars.
    pub fn to_systemd(&self) -> String {
        let mut out = String::new();

        // [Unit]
        out.push_str("[Unit]\n");
        out.push_str(&format!("Description={}\n", self.description));
        if self.after.is_empty() {
            out.push_str("After=network.target\n");
        } else {
            out.push_str(&format!("After=network.target {}\n", self.after.join(" ")));
        }
        if !self.wants.is_empty() {
            out.push_str(&format!("Wants={}\n", self.wants.join(" ")));
        }
        out.push('\n');

        // [Service]
        out.push_str("[Service]\n");
        out.push_str("Type=notify\n");
        out.push_str("NotifyAccess=main\n");
        out.push_str("User={run_user}\n");
        out.push_str("Group={run_group}\n");
        out.push_str(&format!("WorkingDirectory={}\n", self.working_directory));
        out.push_str(&format!(
            "ExecStart={} {}\n",
            self.binary_path, self.config_path
        ));
        for (k, v) in &self.env_vars {
            out.push_str(&format!("Environment={k}={v}\n"));
        }
        if self.no_otel {
            out.push_str("Environment=OTEL_SDK_DISABLED=true\n");
        }
        out.push_str("Restart=on-failure\n");
        out.push_str("RestartSec=5\n");
        out.push_str("KillSignal=SIGTERM\n");
        out.push_str("TimeoutStopSec=45s\n");
        out.push_str("SendSIGKILL=yes\n");
        out.push('\n');

        // [Install]
        out.push_str("[Install]\n");
        out.push_str("WantedBy=multi-user.target\n");

        out
    }
}

pub fn manager_activation_systemd_unit() -> &'static str {
    "[Unit]\nDescription=wruntime manager\nAfter=network.target\n\n[Service]\nType=notify\nNotifyAccess=main\nEnvironmentFile=/var/lib/wruntime/manager-secrets/runtime.env\nExecStart=/usr/local/libexec/wruntime-manager-launch\nRestart=on-failure\nRestartSec=5\nKillSignal=SIGTERM\nTimeoutStopSec=45s\nSendSIGKILL=yes\n\n[Install]\nWantedBy=multi-user.target\n"
}

/// Stable manager launcher. The systemd unit points only at this shim; rollout
/// staging cannot therefore change the executable/config/credential selector.
/// The shim validates the descriptor and every digest immediately before exec.
pub fn manager_launcher_script() -> &'static str {
    r#"#!/usr/bin/env python3
import hashlib, json, os, pathlib, struct, sys
DESCRIPTOR='/var/lib/wruntime/manager-activation/current-activation.json'
def digest_file(path): return 'sha256:'+hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()
def digest_tree(root):
 h=hashlib.sha256(); root=pathlib.Path(root)
 for p in sorted((p for p in root.rglob('*') if p.is_file()), key=lambda p:p.relative_to(root).as_posix()):
  r=p.relative_to(root).as_posix().encode(); b=p.read_bytes(); h.update(struct.pack('>Q',len(r))); h.update(r); h.update(struct.pack('>Q',len(b))); h.update(b)
 return 'sha256:'+h.hexdigest()
d=json.loads(pathlib.Path(DESCRIPTOR).read_text())
if d.get('schema_version') != 1 or d.get('backend') != 'systemd': sys.exit('invalid systemd activation descriptor')
for path,key in [(d['executable'],'executable_digest'),(d['backend_spec_path'],'backend_spec_digest'),(d['config_path'],'config_digest')]:
 if digest_file(path) != d[key]: sys.exit(key+' mismatch')
if digest_tree(d['credential_set_path']) != d['credential_digest']: sys.exit('credential_digest mismatch')
env=os.environ.copy(); env['WRT_MANAGER_CREDENTIAL_SET']=d['credential_set_path']
os.execve(d['executable'],[d['executable'],d['config_path']],env)
"#
}

/// Build the selector-fenced stop for a source activation after target control
/// has been established. The source selector itself remains unchanged.
pub fn manager_source_stop_command(
    manager_id: &str,
    current_descriptor: &str,
    selector_digest: &str,
) -> String {
    fn q(value: &str) -> String {
        super::helpers::shell_quote(value)
    }
    let inspect = r#"import json,sys
d=json.load(open(sys.argv[1]))
if d.get('schema_version') != 1 or d.get('manager_id') != sys.argv[2]: sys.exit('source activation identity mismatch')
print(d.get('backend',''))
print(d.get('backend_spec_path',''))"#;
    format!(
        "set -eu; current={current}; test \"sha256:$(sudo sha256sum -- \"$current\" | cut -d' ' -f1)\" = {selector}; source_info=$(sudo python3 -c {inspect} \"$current\" {manager_id}); backend=$(printf '%s\\n' \"$source_info\" | head -n1); spec=$(printf '%s\\n' \"$source_info\" | tail -n1); case \"$backend\" in systemd) sudo systemctl disable wr-manager.service; sudo systemctl stop wr-manager.service || true; sudo systemctl mask --runtime wr-manager.service || true; if sudo systemctl is-active --quiet wr-manager.service; then echo 'source manager remained active' >&2; exit 1; fi; unit_file_state=$(sudo systemctl is-enabled wr-manager.service || true); test \"$unit_file_state\" = masked-runtime ;; compose) sudo docker compose --project-name wruntime-manager -f \"$spec\" down; test -z \"$(sudo docker compose --project-name wruntime-manager -f \"$spec\" ps -q)\" ;; *) echo 'unsupported source manager backend' >&2; exit 1 ;; esac",
        current = q(current_descriptor),
        selector = q(selector_digest),
        inspect = q(inspect),
        manager_id = q(manager_id),
    )
}

fn manager_systemd_stopped_parser() -> &'static str {
    r#"def require_stopped_systemd(text):
 required={'ActiveState','SubState','MainPID'}; fields={}
 for line in text.splitlines():
  if line.count('=') != 1: sys.exit('malformed manager systemd property')
  key,value=(part.strip() for part in line.split('=',1))
  if key not in required or key in fields or not value: sys.exit('malformed manager systemd property')
  fields[key]=value
 if set(fields) != required: sys.exit('manager systemd property is missing')
 if fields != {'ActiveState':'inactive','SubState':'dead','MainPID':'0'}: sys.exit('manager systemd process is not conclusively stopped')
 return fields"#
}

/// Build a read-only stopped-state inspection for the selected manager activation.
/// The script emits one bounded JSON object containing the installed policy as hex.
pub fn manager_stopped_inspection_command(
    manager_id: &str,
    current_descriptor: &str,
    allowed_selector_digests: &[String],
) -> String {
    fn q(value: &str) -> String {
        super::helpers::shell_quote(value)
    }
    let inspect = format!(
        "{}\n{}",
        manager_systemd_stopped_parser(),
        r#"import hashlib,json,pathlib,subprocess,sys,tomllib
current=pathlib.Path(sys.argv[1]); expected=sys.argv[2]; allowed=set(sys.argv[3:])
raw=current.read_bytes()
selector='sha256:'+hashlib.sha256(raw).hexdigest()
if selector not in allowed: sys.exit('activation selector mismatch')
d=json.loads(raw)
if d.get('schema_version') != 1 or d.get('manager_id') != expected: sys.exit('activation identity mismatch')
required=('backend','backend_spec_path','backend_spec_digest','config_path','config_digest')
if any(not isinstance(d.get(k),str) or not d[k] for k in required): sys.exit('malformed activation descriptor')
config=pathlib.Path(d['config_path'])
expected_config=pathlib.Path('/var/lib/wruntime/manager-config')/expected/'current.toml'
if config != expected_config: sys.exit('activation config path mismatch')
config_raw=config.read_bytes()
if 'sha256:'+hashlib.sha256(config_raw).hexdigest() != d['config_digest']: sys.exit('config digest mismatch')
cfg=tomllib.loads(config_raw.decode('utf-8'))
policy=pathlib.Path(cfg['authorization']['policy_file'])
if not policy.is_absolute(): policy=(config.parent/policy).resolve()
policy_raw=policy.read_bytes()
if len(policy_raw) > 4194304: sys.exit('authorization policy exceeds inspection limit')
backend=d['backend']; spec=pathlib.Path(d['backend_spec_path'])
if backend == 'systemd':
 out=subprocess.run(['systemctl','show','wr-manager.service','--property=ActiveState','--property=SubState','--property=MainPID'],check=True,capture_output=True,text=True).stdout
 require_stopped_systemd(out)
elif backend == 'compose':
 if not spec.is_absolute() or not spec.is_file(): sys.exit('invalid compose backend spec')
 if 'sha256:'+hashlib.sha256(spec.read_bytes()).hexdigest() != d['backend_spec_digest']: sys.exit('compose backend spec digest mismatch')
 out=subprocess.run(['docker','compose','--project-name','wruntime-manager','-f',str(spec),'ps','-q'],check=True,capture_output=True,text=True).stdout
 if out.strip(): sys.exit('manager compose process is not stopped')
else: sys.exit('unsupported manager backend')
print(json.dumps({'manager_id':expected,'selector_digest':selector,'backend':backend,'config_path':str(config),'config_digest':'sha256:'+hashlib.sha256(config_raw).hexdigest(),'policy_path':str(policy),'policy_hex':policy_raw.hex()},sort_keys=True,separators=(',',':')))"#,
    );
    let mut command = format!(
        "sudo python3 -c {inspect} {current} {manager_id}",
        inspect = q(&inspect),
        current = q(current_descriptor),
        manager_id = q(manager_id),
    );
    for digest in allowed_selector_digests {
        command.push(' ');
        command.push_str(&q(digest));
    }
    command
}

/// Build the single post-OLD_CLOSED selector transition. Preparatory config
/// writes may happen first, but the final descriptor rename is authoritative.
pub fn manager_activation_command(
    systemd: bool,
    next_descriptor: &str,
    current_descriptor: &str,
    config_dir: &str,
    config_digest: &str,
    old_selector_digest: &str,
    new_selector_digest: &str,
) -> String {
    fn q(value: &str) -> String {
        super::helpers::shell_quote(value)
    }
    let stop = if systemd {
        "sudo systemctl stop wr-manager.service; sudo systemctl mask --runtime wr-manager.service"
    } else {
        r#"if sudo test -e "$current"; then old_spec=$(sudo python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["backend_spec_path"])' "$current"); sudo docker compose --project-name wruntime-manager -f "$old_spec" down; fi"#
    };
    let start = if systemd {
        "sudo systemctl unmask --runtime wr-manager.service; sudo systemctl unmask wr-manager.service; sudo systemctl enable wr-manager.service; sudo systemctl start wr-manager.service"
    } else {
        // The immutable backend spec is selected by the descriptor; it contains
        // the digest-qualified image and stable mounts/project declaration.
        r#"spec=$(sudo python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["backend_spec_path"])' "$current"); sudo docker compose --project-name wruntime-manager -f "$spec" up -d --force-recreate --no-build"#
    };
    format!(
        "set -eu; current={current}; next={next}; config={config}; test \"sha256:$(sudo sha256sum -- \"$next\" | cut -d' ' -f1)\" = {new}; if sudo test -e \"$current\"; then test \"sha256:$(sudo sha256sum -- \"$current\" | cut -d' ' -f1)\" = {old}; sudo cp --reflink=auto -- \"$current\" \"$current.previous.tmp\"; sudo sync -f \"$current.previous.tmp\"; sudo mv \"$current.previous.tmp\" \"$current.previous\"; fi; {stop}; test \"sha256:$(sudo sha256sum -- \"$config/next.tmp\" | cut -d' ' -f1)\" = {config_digest}; if sudo test -e \"$config/current.toml\"; then sudo cp --reflink=auto -- \"$config/current.toml\" \"$config/previous.tmp\"; sudo chmod 0600 \"$config/previous.tmp\"; sudo sync -f \"$config/previous.tmp\"; sudo mv \"$config/previous.tmp\" \"$config/previous.toml\"; fi; sudo mv \"$config/next.tmp\" \"$config/current.toml\"; sudo chmod 0600 \"$config/current.toml\"; sudo sync -f \"$config\"; sudo mv \"$next\" \"$current\"; sudo chmod 0600 \"$current\"; sudo sync -f $(dirname \"$current\"); {start}",
        current=q(current_descriptor), next=q(next_descriptor), config=q(config_dir), new=q(new_selector_digest), old=q(old_selector_digest), config_digest=q(config_digest), stop=stop, start=start,
    )
}

/// Render the independently installed host node-agent unit. The executor is
/// outside revision-specific engine releases so a rollout cannot replace its
/// own active control process.
pub fn node_agent_systemd_unit(workdir: &str) -> String {
    format!(
        "[Unit]\nDescription=wruntime node lifecycle agent\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nUser=root\nGroup=root\nUMask=0077\nWorkingDirectory={workdir}/wr-agent\nExecStart={workdir}/wr-agent/wr-cli node agent run --config {workdir}/wr-agent/agent.toml\nEnvironment=PATH=\nEnvironment=LANG=C.UTF-8\nNoNewPrivileges=true\nPrivateTmp=true\nPrivateDevices=true\nProtectHome=true\nProtectSystem=strict\nProtectControlGroups=true\nProtectKernelTunables=true\nProtectKernelModules=true\nProtectKernelLogs=true\nProtectClock=true\nProtectHostname=true\nProtectProc=invisible\nRestrictAddressFamilies=AF_UNIX AF_INET AF_INET6\nRestrictNamespaces=true\nRestrictSUIDSGID=true\nLockPersonality=true\nRestrictRealtime=true\nSystemCallArchitectures=native\nRuntimeDirectory=wruntime\nRuntimeDirectoryMode=0700\nReadWritePaths={workdir}/wr-node /run/wruntime /etc/systemd/system -/run/docker.sock -/var/run/docker.sock\nRestart=on-failure\nRestartSec=5\nKillSignal=SIGTERM\nTimeoutStopSec=45s\nSendSIGKILL=yes\n\n[Install]\nWantedBy=multi-user.target\n"
    )
}

/// Render a Dockerfile for a service binary.
pub struct DockerfileSpec<'a> {
    pub workdir: &'a str,
    pub binary: &'a str,
    pub config: &'a str,
    /// Extra COPY lines as `(src, dst)` pairs.
    pub extra_copies: Vec<(&'a str, &'a str)>,
    /// Extra ENV lines as `(key, value)` pairs. Values may contain template vars.
    pub env_vars: Vec<(&'a str, &'a str)>,
    pub no_otel: bool,
}

impl DockerfileSpec<'_> {
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("FROM {DISTROLESS_CC_DEBIAN13}\n"));
        out.push_str(&format!("WORKDIR {}\n", self.workdir));
        out.push_str(&format!("COPY {} {}\n", self.binary, self.binary));
        out.push_str(&format!("COPY {} {}\n", self.config, self.config));
        for (src, dst) in &self.extra_copies {
            out.push_str(&format!("COPY {src} {dst}\n"));
        }
        for (k, v) in &self.env_vars {
            out.push_str(&format!("ENV {k}={v}\n"));
        }
        if self.no_otel {
            out.push_str("ENV OTEL_SDK_DISABLED=true\n");
        }
        out.push_str(&format!(
            "ENTRYPOINT [\"{}\", \"{}\"]\n",
            self.binary, self.config
        ));
        out
    }
}

pub struct ComposeDependency {
    pub service: String,
    pub condition: &'static str,
}

pub struct ComposeHealthcheck {
    pub test: Vec<String>,
    pub interval: &'static str,
    pub timeout: &'static str,
    pub retries: u32,
    pub start_period: &'static str,
}

/// A service entry in a docker-compose file.
pub struct ComposeService {
    pub name: String,
    pub dockerfile: String,
    pub context: String,
    pub image: Option<String>,
    pub network_mode: Option<String>,
    pub ports: Vec<String>,
    pub volumes: Vec<String>,
    pub depends_on: Vec<ComposeDependency>,
    pub healthcheck: ComposeHealthcheck,
}

/// Render a docker-compose.yml from a list of services.
pub fn generate_compose(header: &str, services: &[ComposeService]) -> String {
    let mut out = String::new();
    if !header.is_empty() {
        out.push_str(header);
        out.push('\n');
    }
    out.push_str("services:\n");

    for svc in services {
        out.push_str(&format!(
            "  {}:\n    build:\n      context: {}\n      dockerfile: {}\n",
            svc.name, svc.context, svc.dockerfile
        ));
        if let Some(ref image) = svc.image {
            out.push_str(&format!("    image: {image}\n"));
        }
        if let Some(ref network_mode) = svc.network_mode {
            out.push_str(&format!("    network_mode: {network_mode}\n"));
        }
        if !svc.ports.is_empty() {
            out.push_str("    ports:\n");
            for port in &svc.ports {
                out.push_str(&format!("      - \"{port}\"\n"));
            }
        }
        if !svc.volumes.is_empty() {
            out.push_str("    volumes:\n");
            for volume in &svc.volumes {
                out.push_str(&format!("      - \"{volume}\"\n"));
            }
        }
        if !svc.depends_on.is_empty() {
            out.push_str("    depends_on:\n");
            for dependency in &svc.depends_on {
                out.push_str(&format!(
                    "      {}:\n        condition: {}\n",
                    dependency.service, dependency.condition
                ));
            }
        }
        out.push_str("    healthcheck:\n");
        let healthcheck_test = match serde_json::to_string(&svc.healthcheck.test) {
            Ok(test) => test,
            Err(_) => "[]".to_string(),
        };
        out.push_str(&format!("      test: {healthcheck_test}\n"));
        out.push_str(&format!("      interval: {}\n", svc.healthcheck.interval));
        out.push_str(&format!("      timeout: {}\n", svc.healthcheck.timeout));
        out.push_str(&format!("      retries: {}\n", svc.healthcheck.retries));
        out.push_str(&format!(
            "      start_period: {}\n",
            svc.healthcheck.start_period
        ));
        out.push_str("    stop_signal: SIGTERM\n");
        out.push_str("    stop_grace_period: 45s\n");
        out.push_str("    restart: on-failure\n");
    }

    out
}

/// Sysctl config for wasmtime memory pooling.
pub fn sysctl_config() -> &'static str {
    "# Wasmtime pooling allocator requires higher mmap limit for COW-based instantiation.\nvm.max_map_count = 262144\n"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_definitions_use_semantic_readiness_and_bounded_shutdown() {
        let unit = ServiceUnit {
            description: "test",
            binary_path: "/opt/test",
            config_path: "/opt/test.toml",
            working_directory: "/opt",
            env_vars: vec![],
            no_otel: true,
            after: vec!["wr-proxy.service"],
            wants: vec!["wr-proxy.service"],
        }
        .to_systemd();
        assert!(unit.contains("After=network.target wr-proxy.service\n"));
        assert!(unit.contains("Wants=wr-proxy.service\n"));
        assert!(!unit.contains("Requires="));
        assert!(unit.contains("Type=notify\n"));
        assert!(unit.contains("NotifyAccess=main\n"));
        assert!(unit.contains("KillSignal=SIGTERM\n"));
        assert!(unit.contains("TimeoutStopSec=45s\n"));
        assert!(!unit.contains("Type=simple"));

        let compose = generate_compose(
            "",
            &[ComposeService {
                name: "engine".into(),
                dockerfile: "Dockerfile.engine".into(),
                context: ".".into(),
                image: None,
                network_mode: Some("host".into()),
                ports: vec![],
                volumes: vec![],
                depends_on: vec![ComposeDependency {
                    service: "proxy".into(),
                    condition: "service_healthy",
                }],
                healthcheck: ComposeHealthcheck {
                    test: vec![
                        "CMD".into(),
                        "/opt/engine".into(),
                        "--lifecycle-probe".into(),
                    ],
                    interval: "2s",
                    timeout: "2s",
                    retries: 15,
                    start_period: "30s",
                },
            }],
        );
        assert!(compose.contains("condition: service_healthy"));
        assert!(compose.contains("--lifecycle-probe"));
        assert!(compose.contains("stop_signal: SIGTERM"));
        assert!(compose.contains("stop_grace_period: 45s"));
        assert_eq!(
            compose,
            generate_compose(
                "",
                &[ComposeService {
                    name: "engine".into(),
                    dockerfile: "Dockerfile.engine".into(),
                    context: ".".into(),
                    image: None,
                    network_mode: Some("host".into()),
                    ports: vec![],
                    volumes: vec![],
                    depends_on: vec![ComposeDependency {
                        service: "proxy".into(),
                        condition: "service_healthy",
                    }],
                    healthcheck: ComposeHealthcheck {
                        test: vec![
                            "CMD".into(),
                            "/opt/engine".into(),
                            "--lifecycle-probe".into()
                        ],
                        interval: "2s",
                        timeout: "2s",
                        retries: 15,
                        start_period: "30s",
                    },
                }]
            )
        );
    }

    #[test]
    fn node_agent_is_a_hardened_root_owned_host_service() {
        let unit = node_agent_systemd_unit("/opt/wruntime");
        assert!(unit.contains("User=root\nGroup=root\nUMask=0077\n"));
        assert!(unit.contains("Environment=PATH=\nEnvironment=LANG=C.UTF-8\n"));
        assert!(unit.contains("NoNewPrivileges=true\n"));
        assert!(unit.contains("ProtectSystem=strict\n"));
        assert!(unit.contains("ProtectControlGroups=true\n"));
        assert!(unit.contains("PrivateDevices=true\n"));
        assert!(unit.contains("RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6\n"));
        assert!(unit.contains("RuntimeDirectory=wruntime\nRuntimeDirectoryMode=0700\n"));
        assert!(unit.contains("ReadWritePaths=/opt/wruntime/wr-node /run/wruntime /etc/systemd/system -/run/docker.sock -/var/run/docker.sock\n"));
        assert!(!unit.contains("wr-agent/state"));
        assert!(!unit.contains("ReadWritePaths=/etc "));
        assert!(unit.contains("/opt/wruntime/wr-agent/wr-cli node agent run"));
        assert!(!unit.contains("{run_user}"));
        assert!(!unit.contains("sudo"));
        assert!(!unit.contains("wr-node/releases"));
    }

    #[test]
    fn stopped_inspection_is_selector_bound_and_read_only() {
        let command = manager_stopped_inspection_command(
            "manager-a",
            "/var/lib/wruntime/manager-activation/current-activation.json",
            &[format!("sha256:{}", "1".repeat(64))],
        );
        assert!(command.contains("activation selector mismatch"));
        assert!(command.contains("systemctl"));
        assert!(command.contains("--property=ActiveState"));
        assert!(!command.contains("--value"));
        let parser = manager_systemd_stopped_parser();
        assert!(parser.contains("line.count('=') != 1"));
        assert!(parser.contains("key in fields"));
        assert!(parser.contains("set(fields) != required"));
        assert!(command.contains("ps"));
        assert!(command.contains("-q"));
        assert!(command.contains("policy_hex"));
        for mutation in [
            "systemctl stop",
            "systemctl start",
            "systemctl enable",
            "systemctl disable",
            "systemctl mask",
            "'up'",
            "'down'",
            "shutil",
            "unlink(",
            "write_bytes",
        ] {
            assert!(!command.contains(mutation), "probe contains {mutation}");
        }
    }

    #[test]
    fn stopped_systemd_parser_accepts_reordering_and_rejects_every_invalid_class() {
        let script = format!(
            "import sys\n{}\nrequire_stopped_systemd(sys.argv[1])",
            manager_systemd_stopped_parser()
        );
        let run = |value: &str| {
            std::process::Command::new("python3")
                .args(["-c", &script, value])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .expect("run stopped-state parser")
                .success()
        };
        assert!(run("MainPID=0\nSubState=dead\nActiveState=inactive\n"));
        for rejected in [
            "ActiveState=inactive\nSubState=dead\n",
            "ActiveState=inactive\nSubState=dead\nMainPID=0\nMainPID=0\n",
            "ActiveState=inactive\nSubState\nMainPID=0\n",
            "ActiveState=inactive\nSubState=dead\nMainPID=\n",
            "ActiveState=inactive\nSubState=dead\nUnknown=0\n",
            "ActiveState=active\nSubState=running\nMainPID=42\n",
            "ActiveState=inactive\nSubState=exited\nMainPID=0\n",
            "ActiveState=inactive\nSubState=dead\nMainPID=7\n",
        ] {
            assert!(!run(rejected), "unexpectedly accepted {rejected:?}");
        }
    }

    #[test]
    fn manager_launcher_and_activation_are_digest_gated_and_ordered() {
        let unit = manager_activation_systemd_unit();
        assert_eq!(
            MANAGER_SYSTEMD_UNIT_PATH,
            format!("{MANAGER_SYSTEMD_UNIT_DIR}/wr-manager.service")
        );
        assert!(unit.contains("EnvironmentFile=/var/lib/wruntime/manager-secrets/runtime.env"));
        assert!(!unit.contains("WRT_SECRET_ENCRYPTION_KEY="));

        let launcher = manager_launcher_script();
        assert!(launcher.contains("executable_digest"));
        assert!(launcher.contains("backend_spec_digest"));
        assert!(launcher.contains("credential_digest"));
        assert!(launcher.find("digest_tree").unwrap() < launcher.find("os.execve").unwrap());

        let action = manager_activation_command(
            true,
            "/state/new.next",
            "/state/current-activation.json",
            "/state/config",
            &format!("sha256:{}", "1".repeat(64)),
            &format!("sha256:{}", "2".repeat(64)),
            &format!("sha256:{}", "3".repeat(64)),
        );
        let stop = action.find("systemctl stop").unwrap();
        let select = action.find("mv \"$next\" \"$current\"").unwrap();
        let unmask_runtime = action.find("systemctl unmask --runtime").unwrap();
        let enable = action.find("systemctl enable").unwrap();
        let start = action.find("systemctl start").unwrap();
        assert!(stop < select && select < unmask_runtime);
        assert!(unmask_runtime < enable && enable < start);
        assert!(action.contains("previous.toml"));
        assert!(!action.contains("rm -rf"));

        let source_stop = manager_source_stop_command(
            "manager-a",
            "/state/current-activation.json",
            &format!("sha256:{}", "4".repeat(64)),
        );
        let selector_check = source_stop.find("sha256sum").unwrap();
        let identity_check = source_stop
            .find("source activation identity mismatch")
            .unwrap();
        let disable = source_stop.find("systemctl disable").unwrap();
        let mask = source_stop.find("systemctl mask --runtime").unwrap();
        let source_stop_effect = source_stop.find("systemctl stop").unwrap();
        let inactive = source_stop.find("systemctl is-active").unwrap();
        assert!(selector_check < identity_check);
        assert!(
            identity_check < disable
                && disable < source_stop_effect
                && source_stop_effect < mask
                && mask < inactive
        );
        assert!(source_stop.contains("systemctl stop wr-manager.service || true"));
        assert!(source_stop.contains("systemctl mask --runtime wr-manager.service || true"));
        assert!(source_stop.contains("unit_file_state=$(sudo systemctl is-enabled"));
        assert!(source_stop.contains("source manager remained active"));
        assert!(source_stop.contains("masked-runtime"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn manager_source_stop_fails_closed_when_systemd_stays_active() {
        use sha2::{Digest, Sha256};
        use std::os::unix::fs::PermissionsExt;
        use std::process::Command;

        let directory = tempfile::tempdir().expect("create command test directory");
        let descriptor = directory.path().join("current-activation.json");
        let bytes = br#"{"schema_version":1,"manager_id":"manager-a","backend":"systemd","backend_spec_path":"/unit"}"#;
        std::fs::write(&descriptor, bytes).expect("write activation descriptor");
        let selector_digest = format!("sha256:{:x}", Sha256::digest(bytes));
        let bin = directory.path().join("bin");
        std::fs::create_dir(&bin).expect("create fake binary directory");
        let sudo = bin.join("sudo");
        std::fs::write(&sudo, "#!/bin/sh\nexec \"$@\"\n").expect("write fake sudo");
        std::fs::set_permissions(&sudo, std::fs::Permissions::from_mode(0o700))
            .expect("make fake sudo executable");
        let systemctl = bin.join("systemctl");
        std::fs::write(
            &systemctl,
            r#"#!/bin/sh
case "$1" in
disable) exit 0 ;;
stop|mask) exit 1 ;;
is-active) [ "${MOCK_MANAGER_ACTIVE:-0}" = 1 ] ;;
is-enabled) printf 'masked-runtime\n'; exit 1 ;;
*) exit 2 ;;
esac
"#,
        )
        .expect("write fake systemctl");
        std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o700))
            .expect("make fake systemctl executable");
        let command = manager_source_stop_command(
            "manager-a",
            descriptor.to_str().expect("descriptor path is UTF-8"),
            &selector_digest,
        );
        let path = format!(
            "{}:{}",
            bin.display(),
            std::env::var("PATH").expect("PATH is set")
        );

        let active = Command::new("/bin/sh")
            .args(["-c", &command])
            .env("PATH", &path)
            .env("MOCK_MANAGER_ACTIVE", "1")
            .status()
            .expect("run active source-stop command");
        assert!(!active.success());

        let inactive = Command::new("/bin/sh")
            .args(["-c", &command])
            .env("PATH", &path)
            .env("MOCK_MANAGER_ACTIVE", "0")
            .status()
            .expect("run inactive source-stop command");
        assert!(inactive.success());
    }

    #[test]
    fn dockerfile_base_is_digest_pinned() {
        let rendered = DockerfileSpec {
            workdir: "/opt/wruntime",
            binary: "bin/service",
            config: "config/service.toml",
            extra_copies: vec![],
            env_vars: vec![],
            no_otel: true,
        }
        .render();
        let first_line = rendered.lines().next().expect("Dockerfile has FROM line");
        assert!(first_line.starts_with("FROM gcr.io/distroless/cc-debian13@sha256:"));
        assert_eq!(first_line.matches("sha256:").count(), 1);
    }
}
