import { Effect, Layer, Mailbox, ManagedRuntime, Scope, Stream } from "effect";
import {
	RpcClient,
	RpcClientError,
	RpcMessage,
	RpcSerialization,
	RpcServer,
} from "@effect/rpc";
import { HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { DurableObject } from "cloudflare:workers";
import { User, UserRpcs } from "./rpc";

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

	clientId: number;

	readonly mailbox: Mailbox.Mailbox<{
		readonly clientId: number;
		readonly request: string | Uint8Array;
		readonly controller: ReadableStreamDefaultController;
	}>;

	readonly interrupts: Mailbox.Mailbox<number>;

	/**
	 * The constructor is invoked once upon creation of the Durable Object, i.e. the first call to
	 * 	`DurableObjectStub::get` for a given identifier (no-op constructors can be omitted)
	 *
	 * @param ctx - The interface for interacting with Durable Object state
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 */
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);

		this.clientId = 0;

		this.mailbox = Mailbox.make<{
			readonly clientId: number;
			readonly request: string | Uint8Array;
			readonly controller: ReadableStreamDefaultController;
		}>().pipe(Effect.runSync);

		this.interrupts = Mailbox.make<number>().pipe(Effect.runSync);

		const MainLayer = RpcServer.layer(UserRpcs).pipe(
			Layer.provide(
				UserRpcs.toLayer({
					UserById: ({ id }) => Effect.succeed(new User({ id, name: "Max" })),
					UserCreate: ({ name }) => Effect.succeed(new User({ id: "0", name })),
					UserList: () =>
						Stream.forever(
							Stream.make(new User({ id: "0", name: "Max" })).pipe(
								Stream.tap(() => Effect.sleep("100 millis")),
							),
						),
				}),
			),
			Layer.provide(
				layerProtocolDurableObjectServer(this.mailbox, this.interrupts),
			),
			Layer.provide(RpcSerialization.layerNdjson),
		);

		this.runtime = ManagedRuntime.make(MainLayer);

		// Start the runtime fibers
		this.runtime.runFork(Effect.void);
	}

	async rpc(request: string | Uint8Array): Promise<ReadableStream> {
		const clientId = this.clientId++;
		const mailbox = this.mailbox;
		const interrupts = this.interrupts;
		return new ReadableStream({
			start(controller) {
				mailbox.unsafeOffer({
					clientId,
					request,
					controller,
				});
			},
			cancel() {
				interrupts.unsafeOffer(clientId);
			},
		});
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
	async fetch(request, env, _ctx): Promise<Response> {
		const url = new URL(request.url);

		if (request.method === "POST" && url.pathname === "/rpc") {
			const stub = env.MY_DURABLE_OBJECT.getByName("foo");

			return Effect.gen(function* () {
				const memoMap = yield* Layer.makeMemoMap;
				const scope = yield* Scope.make();

				const rpcClientContext = yield* layerProtocolDurableObject({
					handleRequest: (payload) => stub.rpc(payload),
				}).pipe(
					Layer.provideMerge(RpcSerialization.layerNdjson),
					Layer.provideMerge(Layer.succeed(Scope.Scope, scope)),
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
					Layer.provideMerge(Layer.succeed(Scope.Scope, scope)),
					Layer.buildWithMemoMap(memoMap, scope),
				);

				const handler = yield* RpcServer.toHttpApp(UserRpcs).pipe(
					Effect.provide(rpcHandlerContext),
				);

				const response = yield* handler.pipe(
					Effect.provideService(Scope.Scope, scope),
				);

				return HttpServerResponse.toWeb(response);
			}).pipe(
				Effect.provideService(
					HttpServerRequest.HttpServerRequest,
					HttpServerRequest.fromWeb(request),
				),
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
	) => Promise<ReadableStream>;
}) =>
	RpcClient.Protocol.make(
		Effect.fnUntraced(function* (writeResponse) {
			const serialization = yield* RpcSerialization.RpcSerialization;
			const parser = serialization.unsafeMake();

			const send: (
				request: RpcMessage.FromClientEncoded,
			) => Effect.Effect<void, RpcClientError.RpcClientError> =
				Effect.fnUntraced(function* (request) {
					if (request._tag === "Ping") return;
					const payload = parser.encode(request);
					if (payload === undefined) return;
					const result = yield* Effect.promise(() =>
						options.handleRequest(payload),
					);
					let reader = result.getReader();
					const read = Effect.promise(() => reader.read());
					while (true) {
						const response = yield* read;
						if (response.done) {
							break;
						}
						const decoded = parser.decode(
							response.value,
						) as Array<RpcMessage.FromServerEncoded>;
						for (const message of decoded) {
							yield* writeResponse(message);
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
	) => Promise<ReadableStream>;
}) => Layer.effect(RpcClient.Protocol, makeProtocolDurableObject(options));

const makeProtocolDurableObjectServer = Effect.fnUntraced(function* (
	mailbox: Mailbox.Mailbox<{
		readonly clientId: number;
		readonly request: string | Uint8Array;
		readonly controller: ReadableStreamDefaultController;
	}>,
	interrupts: Mailbox.Mailbox<number>,
) {
	const serialization = yield* RpcSerialization.RpcSerialization;
	const parser = serialization.unsafeMake();

	const disconnects = yield* Mailbox.make<number>();
	let writeRequest!: (
		clientId: number,
		message: RpcMessage.FromClientEncoded,
	) => Effect.Effect<void>;

	const clients = new Map<
		number,
		{
			readonly clientId: number;
			readonly request: RpcMessage.RequestEncoded;
			readonly controller: ReadableStreamDefaultController;
		}
	>();

	yield* mailbox.take.pipe(
		Effect.flatMap(
			Effect.fnUntraced(function* (request) {
				const decoded = parser.decode(
					request.request,
				) as ReadonlyArray<RpcMessage.FromClientEncoded>;

				const message = decoded[0];

				if (message._tag !== "Request") return;

				clients.set(request.clientId, {
					...request,
					request: message,
				});

				yield* writeRequest(request.clientId, message);
				yield* writeRequest(request.clientId, RpcMessage.constEof);
			}),
		),
		Effect.forever,
		Effect.interruptible,
		Effect.forkScoped,
	);

	yield* interrupts.take.pipe(
		Effect.flatMap(
			Effect.fnUntraced(function* (clientId) {
				const client = clients.get(clientId);
				if (!client) return;
				yield* writeRequest(clientId, {
					_tag: "Interrupt",
					requestId: client.request.id,
				});
			}),
		),
		Effect.forever,
		Effect.interruptible,
		Effect.forkScoped,
	);

	const protocol = yield* RpcServer.Protocol.make((writeRequest_) => {
		writeRequest = writeRequest_;
		return Effect.succeed({
			disconnects,
			clientIds: Effect.sync(() => clients.keys()),
			send: (clientId, response) => {
				const client = clients.get(clientId);
				if (!client) return Effect.void;
				const encoded = parser.encode(response);
				if (!encoded) return Effect.void;
				client.controller.enqueue(
					typeof encoded === "string"
						? new TextEncoder().encode(encoded)
						: encoded,
				);
				return Effect.void;
			},
			end(clientId) {
				const client = clients.get(clientId);
				if (!client) return Effect.void;
				client.controller.close();
				clients.delete(clientId);
				return Effect.void;
			},
			initialMessage: Effect.succeedNone,
			supportsAck: false,
			supportsTransferables: false,
			supportsSpanPropagation: false,
		});
	});

	return protocol;
});

const layerProtocolDurableObjectServer = (
	mailbox: Mailbox.Mailbox<{
		readonly clientId: number;
		readonly request: string | Uint8Array;
		readonly controller: ReadableStreamDefaultController;
	}>,
	interrupts: Mailbox.Mailbox<number>,
) =>
	Layer.scoped(
		RpcServer.Protocol,
		makeProtocolDurableObjectServer(mailbox, interrupts),
	);
