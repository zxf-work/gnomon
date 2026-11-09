#!/usr/bin/env python3
"""Writes cypher-override/window/IS4.cypher: the kit's window text of IS4 (also read for TLDBC-IS4) with its two RETURN items in
the order of the instant text (messageContent, messageCreationDate), the order of the SQL reference and of Gnomon. Values and
logic are untouched. Run once; the result is committed."""
import os
import re
import sys
from pathlib import Path

repo = Path(os.environ["EXTCMP_REPO"])
src = repo / "benchmarks/workloads/snb/cypher/window/IS4.cypher"
dst = Path(__file__).resolve().parents[1] / "cypher-override/window/IS4.cypher"
text = src.read_text()
body = "\n".join(l for l in text.splitlines() if not l.strip().startswith("//"))
assert len(re.findall(r"\bRETURN\b", body)) == 1, "expected exactly one RETURN"
m = re.search(r"\bRETURN\b(.*?)(\n\s*(ORDER BY|LIMIT)\b.*)?\Z", text, re.S)
head, items_txt, tail = text[:m.start()], m.group(1), m.group(2) or ""
items, depth, cur = [], 0, ""
for ch in items_txt:
    depth += ch in "([{"
    depth -= ch in ")]}"
    if ch == "," and depth == 0:
        items.append(cur.strip()); cur = ""
    else:
        cur += ch
items.append(cur.strip())
assert len(items) == 2, f"expected 2 RETURN items, got {items}"
ci = [i for i, x in enumerate(items) if "creationDate" in x.split(" AS ")[0].split(" as ")[0]]
assert len(ci) == 1, items
if ci[0] == 1:
    sys.exit("window IS4 already returns the creation date last: nothing to override")
items = [items[1], items[0]]
out = head + "RETURN\n    " + ",\n    ".join(items) + tail.rstrip() + "\n"
out = "// W3 stage override: the two RETURN items in the order of the instant IS4 text and the SQL reference.\n" + out
dst.parent.mkdir(parents=True, exist_ok=True)
dst.write_text(out)
print("---- kit text\n" + text + "\n---- override\n" + out)
