import { describe, it, expect } from "vitest";
import { encrypt, decrypt } from "../src/vault/crypto.js";

describe("encrypt / decrypt", () => {
  it("roundtrip: decrypted output equals original plaintext", () => {
    const plaintext = Buffer.from("hello db-mirror");
    const blob = encrypt(plaintext, "passphrase");
    expect(decrypt(blob, "passphrase")).toEqual(plaintext);
  });

  it("produces different ciphertext for the same input (random IV+salt)", () => {
    const pt = Buffer.from("same");
    expect(encrypt(pt, "pw").toString("hex")).not.toBe(encrypt(pt, "pw").toString("hex"));
  });

  it("preserves binary content through roundtrip", () => {
    const binary = Buffer.from([0x00, 0xff, 0xab, 0xcd, 0xef]);
    expect(decrypt(encrypt(binary, "k"), "k")).toEqual(binary);
  });

  it("throws on wrong passphrase", () => {
    const blob = encrypt(Buffer.from("secret"), "correct");
    expect(() => decrypt(blob, "wrong")).toThrow(/invalid passphrase|corrupted/i);
  });

  it("throws when blob is too short", () => {
    expect(() => decrypt(Buffer.alloc(10), "x")).toThrow(/blob too short/);
  });

  it("throws on bad magic bytes", () => {
    const blob = encrypt(Buffer.from("x"), "pw");
    blob[0] = 0x00;
    expect(() => decrypt(blob, "pw")).toThrow(/bad magic|unsupported format/i);
  });

  it("empty plaintext roundtrips cleanly", () => {
    const empty = Buffer.alloc(0);
    expect(decrypt(encrypt(empty, "pw"), "pw")).toEqual(empty);
  });
});
