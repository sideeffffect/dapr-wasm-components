#!/usr/bin/env python3
"""Work around a wit-bindgen (Go) name clash in the generated bindings.

`jobs.job-failure-policy` has a case `constant(job-failure-policy-constant)`:
the generator names the case's tag constant `JobFailurePolicyConstant`, the
same identifier as the record type, which does not compile. Rename the tag
constant to `JobFailurePolicyTagConstant`. Every replacement is asserted, so
a generator fix (or change) fails loudly here instead of silently.
"""
import sys

JOBS = "internal/wit/dapr_wasm_components_interfaces_jobs/wit_bindings.go"
EXPORTS = "internal/wit/wit_exports/wit_exports.go"
FIXES = {
    JOBS: [
        ("JobFailurePolicyConstant uint8 = 1", "JobFailurePolicyTagConstant uint8 = 1", 1),
        ("self.tag != JobFailurePolicyConstant {", "self.tag != JobFailurePolicyTagConstant {", 1),
        ("JobFailurePolicy{JobFailurePolicyConstant, value}", "JobFailurePolicy{JobFailurePolicyTagConstant, value}", 1),
    ],
    EXPORTS: [
        ("case dapr_wasm_components_interfaces_jobs.JobFailurePolicyConstant:",
         "case dapr_wasm_components_interfaces_jobs.JobFailurePolicyTagConstant:", None),
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
