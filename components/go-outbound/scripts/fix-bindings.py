#!/usr/bin/env python3
"""Work around two wit-bindgen (Go) issues in the generated bindings.

1. `jobs-read.job-failure-policy` has a case `constant(job-failure-policy-constant)`:
   the generator names the case's tag constant `JobFailurePolicyConstant`, the
   same identifier as the record type, which does not compile. Rename the tag
   constant to `JobFailurePolicyTagConstant`.
2. `<Resource>FromOwnHandle` (an own handle of an exported resource coming
   back in, e.g. `encryptor.finish(this)`) returns the object without
   recording the new handle, so `Drop()` would release a stale one and the
   incoming handle could never be dropped. Record it.

Every replacement is asserted, so a generator fix (or change) fails loudly
here instead of silently.
"""
import sys

JOBS = "internal/wit/dapr_wasm_components_interfaces_jobs_read/wit_bindings.go"
EXPORTS = "internal/wit/wit_exports/wit_exports.go"
CRYPTO = "internal/wit/export_dapr_wasm_components_interfaces_crypto/wit_bindings.go"


def from_own(name):
    old = (f"func {name}FromOwnHandle(handle int32) *{name} {{\n"
           f"\treturn (*{name})(unsafe.Pointer(resourceRep{name}(handle)))\n}}")
    new = (f"func {name}FromOwnHandle(handle int32) *{name} {{\n"
           f"\tself := (*{name})(unsafe.Pointer(resourceRep{name}(handle)))\n"
           f"\tself.handle = handle\n\treturn self\n}}")
    return (old, new, 1)


FIXES = {
    JOBS: [
        ("JobFailurePolicyConstant uint8 = 1", "JobFailurePolicyTagConstant uint8 = 1", 1),
        ("self.tag != JobFailurePolicyConstant {", "self.tag != JobFailurePolicyTagConstant {", 1),
        ("JobFailurePolicy{JobFailurePolicyConstant, value}", "JobFailurePolicy{JobFailurePolicyTagConstant, value}", 1),
    ],
    CRYPTO: [from_own("Encryptor"), from_own("Decryptor"), from_own("CryptoOutput")],
    EXPORTS: [
        ("case dapr_wasm_components_interfaces_jobs_read.JobFailurePolicyConstant:",
         "case dapr_wasm_components_interfaces_jobs_read.JobFailurePolicyTagConstant:", None),
    ],
}

for path, fixes in FIXES.items():
    text = open(path).read()
    for old, new, expected in fixes:
        n = text.count(old)
        if n == 0 or (expected is not None and n != expected):
            sys.exit(f"fix-bindings: {path}: expected {expected or '>=1'} of {old!r}, found {n}")
        text = text.replace(old, new)
    open(path, "w").write(text)
