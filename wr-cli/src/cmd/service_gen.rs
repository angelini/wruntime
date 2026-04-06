//! Parameterized generators for systemd units, Dockerfiles, and docker-compose files.
//!
//! Template variables like `{run_user}`, `{run_group}`, `{secret_key}` are emitted
//! as literal `{...}` strings and resolved later by `helpers::resolve_template()`.

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
        out.push_str("Type=simple\n");
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
        out.push('\n');

        // [Install]
        out.push_str("[Install]\n");
        out.push_str("WantedBy=multi-user.target\n");

        out
    }
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
        out.push_str("FROM gcr.io/distroless/cc-debian13\n");
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

/// A service entry in a docker-compose file.
pub struct ComposeService {
    pub name: String,
    pub dockerfile: String,
    pub context: String,
    pub image: Option<String>,
    pub ports: Vec<String>,
    pub depends_on: Vec<String>,
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
        if !svc.ports.is_empty() {
            out.push_str("    ports:\n");
            for port in &svc.ports {
                out.push_str(&format!("      - \"{port}\"\n"));
            }
        }
        if !svc.depends_on.is_empty() {
            out.push_str("    depends_on:\n");
            for dep in &svc.depends_on {
                out.push_str(&format!("      - {dep}\n"));
            }
        }
        out.push_str("    restart: on-failure\n");
    }

    out
}

/// Sysctl config for wasmtime memory pooling.
pub fn sysctl_config() -> &'static str {
    "# Wasmtime pooling allocator requires higher mmap limit for COW-based instantiation.\nvm.max_map_count = 262144\n"
}
