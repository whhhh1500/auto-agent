// Package artifactmigration defines the durable, bounded contract for a
// resource-object migration. It deliberately has no object-store client or
// copier implementation: workers live behind this contract.
package artifactmigration
