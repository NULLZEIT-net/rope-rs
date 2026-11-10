"""Runs the ROPE conformance suite against this implementation and checks the scenarios listed in expected.txt.

    python conformance/run.py --rope ../rope                 # every scenario in expected.txt must pass
    python conformance/run.py --rope ../rope --all           # also show what does not pass yet

The suite lives in the rope repository (conformance/harness.py); the adapter is the binary
`rope-conformance-adapter` of this crate (cargo build --release). Exit code 0 only if every expected scenario passed.
"""

import argparse
import asyncio
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--rope", default=os.path.join(HERE, "..", "..", "rope"), help="checkout of the rope repository")
    ap.add_argument("--adapter", help="adapter command (default: target/release/rope-conformance-adapter)")
    ap.add_argument("--all", action="store_true", help="run every scenario, not only the expected ones")
    args = ap.parse_args()

    sys.path.insert(0, os.path.join(os.path.abspath(args.rope), "conformance"))
    import harness  # noqa: E402 - from the rope checkout

    exe = "rope-conformance-adapter" + (".exe" if os.name == "nt" else "")
    adapter = args.adapter or '"%s"' % os.path.join(HERE, "..", "target", "release", exe)
    with open(os.path.join(HERE, "expected.txt"), encoding="utf-8") as f:
        expected = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    known = {sc.__name__ for sc in harness.SCENARIOS}
    missing = [n for n in expected if n not in known]
    if missing:
        print("not in the suite of this rope checkout: %s" % ", ".join(missing))
        return 1
    if not args.all:
        harness.SCENARIOS[:] = [sc for sc in harness.SCENARIOS if sc.__name__ in expected]
    results = asyncio.run(harness.run(adapter))
    passed = {name for name, res, _t, _why in results if res == "PASS"}
    failed = [n for n in expected if n not in passed]
    print("%d of %d expected scenarios passed (suite: %d scenarios)" % (len(expected) - len(failed), len(expected),
                                                                       len(known)))
    if failed:
        print("failed: %s" % ", ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
