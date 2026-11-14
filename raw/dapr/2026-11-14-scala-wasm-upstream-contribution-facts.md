# scala-wasm: how upstream contributions went, and what an async flag touches

> Source: scala-wasm/scala-wasm pull requests #268–#271 and the open PR list (read with `gh` on 2026-11-14); scala-wasm source at `d4e4feb`
> Collected: 2026-11-14
> Published: 2026-11-14

## Pull request history (the four PRs that came from dapr4s, opened 2026-10-13)

- #268 "Pass WIT params through linear memory beyond 16 flat params": +169/−4, 8 files. Merged 2026-10-15 (2 days).
- #269 "Keep @WitAlias behind a plain Scala type alias": +132, 9 files. Merged 2026-11-09 (27 days, after a ping on 2026-11-05). Maintainer, 2026-10-15: "I'd also like to change how WIT aliases are represented… I'll open an issue for that".
- #271 "sbt plugin: exclude the upstream org.scala-js core libraries": +130, 7 files. Merged 2026-11-09 (27 days, after a ping).
- #270 "Plug System.out, clocks and Math.random on Wasm without JS (wasmlibintf)": +305/−47, 13 files. Closed 2026-10-15, no review. Maintainer: "For contributions around javalib, references to the OpenJDK implementation are strictly prohibited, so AI contributions need to be avoided… I'll probably reimplement it myself". 2026-10-17: "rewriting the same changes by hand and opening a new PR would carry essentially the same potential license risk, so please refrain from doing that" and "I'm not yet satisfied with this implementation… I'd like to improve the design a bit more".
- All reviews and merges came from one maintainer. Open PRs on 2026-11-14: only the automatic upstream sync (#233).
- The repository has no `LLM_POLICY.md`. `CONTRIBUTING.md` asks for the Scala CLA, the coding style, and a review by at least one core developer.

## What an async flag on the WIT function type touches

- `wit.FuncType` is part of the serialized IR: `ir/.../Serializers.scala` writes it with `TagWITFuncType` and reads it in `readWITFuncType()`. The IR binary version is `1.22+wasm` (`ScalaJSVersions.scala`).
- The Scala 2 compiler plugin builds it in `compiler/.../nscplugin/GenWitInterop.scala` (four places).
- wit-bindgen-scala (`scala-wasm/wit-bindgen-scala`, a separate repository) has no `async` handling in its sources.
- So the change spans: an annotation or annotation parameter, the compiler plugin, the IR serializer (format change), the linker's component type writer, and wit-bindgen-scala.
