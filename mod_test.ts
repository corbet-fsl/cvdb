import { parseJsonLines } from "./mod.ts";

Deno.test("blank lines, CRLF and Unicode preserve records", () => {
  const values = parseJsonLines('\n{"name":"雪"}\r\n \r\nnull\n[1,true]\n');
  if (
    JSON.stringify(values) !== JSON.stringify([{ name: "雪" }, null, [1, true]])
  ) {
    throw new Error("Records changed");
  }
});

Deno.test("malformed JSON is rejected", () => {
  try {
    parseJsonLines("1\n{invalid}\n2\n");
  } catch (error) {
    if (error instanceof SyntaxError) return;
    throw error;
  }
  throw new Error("Malformed input was accepted");
});
