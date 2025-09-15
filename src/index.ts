import { ReadableStream } from "@cloudflare/workers-types";
import { Console, Effect, Layer, ManagedRuntime, Stream } from "effect";
import {
	Rpc,
	RpcClient,
	RpcClientError,
	RpcGroup,
	RpcMessage,
	RpcSerialization,
	RpcServer,
} from "@effect/rpc";
import { HttpApp, Headers } from "@effect/platform";
import { DurableObject } from "cloudflare:workers";
import { User, UserRpcs } from "./rpc";
import { constVoid } from "effect/Function";

/**
 * Welcome to Cloudflare Workers! This is your first Durable Objects application.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your Durable Object in action
 * - Run `npm run deploy` to publish your application
 *
 * Bind resources to your worker in `wrangler.jsonc`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/durable-objects
 */

/** A Durable Object's behavior is defined in an exported Javascript class */
export class MyDurableObject extends DurableObject<Env> {
	readonly runtime: ManagedRuntime.ManagedRuntime<never, never>;

	/**
	 * The constructor is invoked once upon creation of the Durable Object, i.e. the first call to
	 * 	`DurableObjectStub::get` for a given identifier (no-op constructors can be omitted)
	 *
	 * @param ctx - The interface for interacting with Durable Object state
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 */
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.runtime = ManagedRuntime.make(Layer.empty);
	}

	/**
	 * The Durable Object exposes an RPC method sayHello which will be invoked when when a Durable
	 *  Object instance receives a request from a Worker via the same method invocation on the stub
	 *
	 * @param name - The name provided to a Durable Object instance from a Worker
	 * @returns The greeting to be sent back to the Worker
	 */
	async sayHello(name: string): Promise<string> {
		return `Hello, ${name}!`;
	}

	async rpc(
		payload: string | Uint8Array,
	): Promise<Uint8Array | ReadableStream> {
		return Effect.gen(function* () {
			const handlersContext = yield* UserRpcs.toLayer({
				UserById: ({ id }) => Effect.succeed(new User({ id, name: "Max" })),
				UserCreate: ({ name }) => Effect.succeed(new User({ id: "0", name })),
				UserList: () =>
					Stream.forever(
						Stream.make(new User({ id: "0", name: "Max" })).pipe(
							Stream.tap(() => Effect.sleep("100 millis")),
						),
					),
			}).pipe(Layer.build);

			const parser = RpcSerialization.ndjson.unsafeMake();

			const requests = parser.decode(
				payload,
			) as Array<RpcMessage.FromClientEncoded>;
			const request = requests[0];

			if (request._tag === "Request") {
				const handler = yield* UserRpcs.accessHandler(request.tag as any).pipe(
					Effect.provide(handlersContext),
				);

				const response = handler(
					request.payload as any,
					Headers.fromInput(request.headers),
				);
				if (Effect.isEffect(response)) {
					return yield* Effect.map(response, (user) => {
						return parser.encode(user);
					});
				}
			}

			return yield* Effect.succeed(new Uint8Array());
		}).pipe(Effect.scoped, Effect.runPromise);
	}
}

export default {
	/**
	 * This is the standard fetch handler for a Cloudflare Worker
	 *
	 * @param request - The request submitted to the Worker from the client
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 * @param ctx - The execution context of the Worker
	 * @returns The response to be sent back to the client
	 */
	async fetch(request, env, ctx): Promise<Response> {
		const url = new URL(request.url);

		if (request.method === "POST" && url.pathname === "/rpc") {
			const stub = env.MY_DURABLE_OBJECT.getByName("foo");

			return Effect.gen(function* () {
				const memoMap = yield* Layer.makeMemoMap;
				const scope = yield* Effect.scope;

				const rpcClientContext = yield* layerProtocolDurableObject({
					handleRequest: (payload) => stub.rpc(payload),
				}).pipe(
					Layer.provideMerge(RpcSerialization.layerNdjson),
					Layer.buildWithMemoMap(memoMap, scope),
				);

				const rpcClient = yield* RpcClient.make(UserRpcs).pipe(
					Effect.provide(rpcClientContext),
				);

				const rpcHandlerContext = yield* UserRpcs.toLayer({
					UserById: (params) => Effect.orDie(rpcClient.UserById(params)),
					UserCreate: (params) => Effect.orDie(rpcClient.UserCreate(params)),
					UserList: () => Stream.orDie(rpcClient.UserList()),
				}).pipe(
					Layer.provideMerge(RpcServer.layerProtocolHttp({ path: "/rpc" })),
					Layer.provideMerge(RpcSerialization.layerNdjson),
					Layer.buildWithMemoMap(memoMap, scope),
				);

				const handler = yield* RpcServer.toHttpApp(UserRpcs).pipe(
					Effect.map(HttpApp.toWebHandler),
					Effect.provide(rpcHandlerContext),
				);

				return yield* Effect.promise(() => handler(request));
			}).pipe(
				Effect.scoped,
				Effect.tapErrorCause(Effect.logError),
				Effect.runPromise,
			);
		}

		return new Response("Not found", { status: 404 });
	},
} satisfies ExportedHandler<Env>;

const makeProtocolDurableObject = (options: {
	readonly handleRequest: (
		payload: string | Uint8Array,
	) => Promise<Uint8Array | ReadableStream>;
}) =>
	RpcClient.Protocol.make(
		Effect.fnUntraced(function* (writeResponse) {
			const serialization = yield* RpcSerialization.RpcSerialization;
			const parser = serialization.unsafeMake();

			const send: (
				request: RpcMessage.FromClientEncoded,
			) => Effect.Effect<void, RpcClientError.RpcClientError> =
				Effect.fnUntraced(function* (request) {
					switch (request._tag) {
						case "Request": {
							const payload = parser.encode(request);
							if (payload === undefined) {
								return yield* Effect.void;
							}
							const result = yield* Effect.promise(() =>
								options.handleRequest(payload),
							);
							if (result instanceof Uint8Array) {
								const responses = parser.decode(
									result,
								) as Array<RpcMessage.FromServerEncoded>;
								if (responses.length === 0) {
									return yield* Effect.void;
								}
								let i = 0;
								return yield* Effect.whileLoop({
									while: () => i < responses.length,
									body: () => writeResponse(responses[i++]),
									step: constVoid,
								});
							}
							break;
						}
						case "Interrupt": {
							break;
						}
					}
				});

			return {
				send,
				supportsAck: false,
				supportsTransferables: false,
			};
		}),
	);

const layerProtocolDurableObject = (options: {
	readonly handleRequest: (
		payload: string | Uint8Array,
	) => Promise<Uint8Array | ReadableStream>;
}) => Layer.effect(RpcClient.Protocol, makeProtocolDurableObject(options));
