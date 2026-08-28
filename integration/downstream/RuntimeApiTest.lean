import Http2.Runtime

def main : IO Unit := do
  let token ← Std.CancellationToken.new
  unless ← Http2.CancellationToken.cancel token do
    throw <| IO.userError "fresh cancellation token was already cancelled"
