import Lake
open Lake DSL

/-!
# Editor project model

Bazel owns release builds and tests. This Lake package supplies the source
model used by editors and `lake serve`.
-/

package «http2-lean» where
  version := v!"0.1.0"
  description := "HTTP/2 protocol foundations for Lean 4"
  keywords := #["http2", "hpack", "networking", "protocol"]
  leanOptions := #[⟨`experimental.module, true⟩]

require «tls13-lean» from git
  "https://github.com/pb64-lean/tls13-lean.git" @
  "4fa14fe068d9ea17c85294a6a27c224b2de5cddb"

@[default_target]
lean_lib «Http2» where
  srcDir := "lean"
  roots := #[`Http2, `Http2.Runtime]
