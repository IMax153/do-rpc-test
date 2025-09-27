import { RpcClient, RpcSerialization } from "@effect/rpc";
import { FetchHttpClient } from "@effect/platform";
import { Console, Effect, Layer, Stream } from "effect";
import { UserRpcs } from "./rpc";

const ProtocolLive = RpcClient.layerProtocolHttp({
	url: "http://localhost:8787/rpc",
}).pipe(
	Layer.provide([
		// use fetch for http requests
		FetchHttpClient.layer,
		// use ndjson for serialization
		RpcSerialization.layerNdjson,
	]),
);

// Use the client
const program = Effect.gen(function* () {
	const client = yield* RpcClient.make(UserRpcs);
	yield* client.UserList().pipe(
		Stream.take(3),
		Stream.runForEach((user) => Console.log(user)),
	);
}).pipe(Effect.scoped);

program.pipe(
	Effect.provide(ProtocolLive),
	Effect.catchAllCause(Effect.logError),
	Effect.runPromise,
);
