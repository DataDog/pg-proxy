// Package prepared tracks client↔proxy prepared-statement name mappings for
// transaction-level pooling: pending Parse messages, confirmed named
// statements, and the auto-cache of unnamed statements onto named proxy names.
package prepared
