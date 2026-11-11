//! A startup ceiling on tool selection and construction, independent of session identity.
//! This policy only restricts tools; permission and approval checks still apply.

use crate::ToolName;

/// Supply through `ExtensionDataInit` before starting a thread. The host captures
/// the policy once; later extension-state changes cannot relax it. Callers must
/// supply it again when resuming a thread.
#[derive(Clone, Debug)]
pub struct ToolPolicy {
    /// `None` keeps ordinary tool selection; an empty list permits no tools.
    /// Names include their namespace; plain names use the default namespace.
    /// Generated tools such as Code Mode's `exec` and `wait` must also be listed.
    pub allowed_tools: Option<Vec<ToolName>>,
    /// Omit core tools unless the thread and every ready environment use a managed sandbox.
    pub require_managed_sandbox: bool,
    /// Omit shell tools when unified exec is disabled, instead of using one-shot exec.
    pub require_unified_exec: bool,
    /// Advertise additional-permission arguments when the feature is enabled.
    pub expose_additional_permissions: bool,
}

impl Default for ToolPolicy {
    fn default() -> Self {
        Self {
            allowed_tools: None,
            require_managed_sandbox: false,
            require_unified_exec: false,
            expose_additional_permissions: true,
        }
    }
}

impl ToolPolicy {
    /// Combine independent ceilings without granting a tool or relaxing execution.
    pub fn intersect(&self, ceiling: &Self) -> Self {
        let allowed_tools = match (&self.allowed_tools, &ceiling.allowed_tools) {
            (None, None) => None,
            (Some(tools), None) | (None, Some(tools)) => Some(tools.clone()),
            (Some(tools), Some(_)) => Some(
                tools
                    .iter()
                    .filter(|tool| ceiling.allows(tool))
                    .cloned()
                    .collect(),
            ),
        };
        Self {
            allowed_tools,
            require_managed_sandbox: self.require_managed_sandbox
                || ceiling.require_managed_sandbox,
            require_unified_exec: self.require_unified_exec || ceiling.require_unified_exec,
            expose_additional_permissions: self.expose_additional_permissions
                && ceiling.expose_additional_permissions,
        }
    }

    pub fn allows(&self, tool: &ToolName) -> bool {
        self.allowed_tools.as_ref().is_none_or(|tools| {
            tools.iter().any(|allowed| {
                allowed.name == tool.name
                    && (allowed.namespace == tool.namespace
                        || (allowed.is_default_namespace() && tool.is_default_namespace()))
            })
        })
    }
}
