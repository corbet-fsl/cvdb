/** JSON Lines text parsing utilities. @module */

/** Parse nonblank JSON Lines records; malformed JSON throws a SyntaxError. */
export function parseJsonLines(text: string): unknown[] {
  return text.split(/\r?\n/)
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line) as unknown);
}
