package warden

// Identifier identifies a resource uniquely among resources of the same kind.
// For example, a database uses its name, while a user may use "<email>|<group>".
type Identifier string
