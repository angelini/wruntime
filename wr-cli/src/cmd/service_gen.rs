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
    /// `Requires=` dependencies.
    pub requires: Vec<&'a str>,
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
        if !self.requires.is_empty() {
            out.push_str(&format!("Requires={}\n", self.requires.join(" ")));
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
    "[Unit]\nDescription=wruntime manager\nAfter=network.target\n\n[Service]\nType=notify\nNotifyAccess=main\nExecStart=/usr/local/libexec/wruntime-manager-launch\nRestart=on-failure\nRestartSec=5\nKillSignal=SIGTERM\nTimeoutStopSec=45s\nSendSIGKILL=yes\n\n[Install]\nWantedBy=multi-user.target\n"
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
        "sudo systemctl unmask wr-manager.service; sudo systemctl start wr-manager.service"
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
        "[Unit]\nDescription=wruntime node lifecycle agent\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nUser=root\nGroup=root\nUMask=0077\nWorkingDirectory={workdir}/wr-agent\nExecStart={workdir}/wr-agent/wr-cli node agent run --config {workdir}/wr-agent/agent.toml\nEnvironment=PATH=\nEnvironment=LANG=C.UTF-8\nNoNewPrivileges=true\nPrivateTmp=true\nPrivateDevices=true\nProtectHome=true\nProtectSystem=strict\nProtectControlGroups=true\nProtectKernelTunables=true\nProtectKernelModules=true\nProtectKernelLogs=true\nProtectClock=true\nProtectHostname=true\nProtectProc=invisible\nRestrictAddressFamilies=AF_UNIX AF_INET AF_INET6\nRestrictNamespaces=true\nRestrictSUIDSGID=true\nLockPersonality=true\nRestrictRealtime=true\nSystemCallArchitectures=native\nRuntimeDirectory=wruntime\nRuntimeDirectoryMode=0700\nReadWritePaths={workdir}/wr-node {workdir}/wr-agent/state /run/wruntime /etc/systemd/system -/run/docker.sock -/var/run/docker.sock\nRestart=on-failure\nRestartSec=5\nKillSignal=SIGTERM\nTimeoutStopSec=45s\nSendSIGKILL=yes\n\n[Install]\nWantedBy=multi-user.target\n"
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
            requires: vec!["wr-proxy.service"],
        }
        .to_systemd();
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
        assert!(unit.contains("ReadWritePaths=/opt/wruntime/wr-node /opt/wruntime/wr-agent/state /run/wruntime /etc/systemd/system -/run/docker.sock -/var/run/docker.sock\n"));
        assert!(!unit.contains("ReadWritePaths=/etc "));
        assert!(unit.contains("/opt/wruntime/wr-agent/wr-cli node agent run"));
        assert!(!unit.contains("{run_user}"));
        assert!(!unit.contains("sudo"));
        assert!(!unit.contains("wr-node/releases"));
    }

    #[test]
    fn manager_launcher_and_activation_are_digest_gated_and_ordered() {
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
        let start = action.find("systemctl start").unwrap();
        assert!(stop < select && select < start);
        assert!(action.contains("previous.toml"));
        assert!(!action.contains("rm -rf"));
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
