import { Effect, Layer, Logger, LogLevel, ManagedRuntime, Schema } from "effect"
import { Rpc, RpcClient, RpcGroup, RpcSerialization, RpcServer } from "@effect/rpc"
import { HttpServer } from "@effect/platform"
import { DurableObject } from "cloudflare:workers";

const MyRpcs = RpcGroup.make(
	Rpc.make("Echo", {
		success: Schema.Struct({ text: Schema.String }),
		payload: { text: Schema.String }
	})
)

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
	readonly runtime: ManagedRuntime.ManagedRuntime<never, never>

	/**
	 * The constructor is invoked once upon creation of the Durable Object, i.e. the first call to
	 * 	`DurableObjectStub::get` for a given identifier (no-op constructors can be omitted)
	 *
	 * @param ctx - The interface for interacting with Durable Object state
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 */
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.runtime = ManagedRuntime.make(Layer.empty)
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

	async rpc(): Promise<Uint8Array | ReadableStream> {
		return Promise.resolve(new Uint8Array())
	}
}

const MyRpcsLayer = MyRpcs.toLayer({
	Echo: ({ text }) => Effect.succeed({ text })
})

const { handler } = RpcServer.toWebHandler(MyRpcs, {
	layer: Layer.mergeAll(
		MyRpcsLayer,
		RpcSerialization.layerJson,
		HttpServer.layerContext
	).pipe(
		Layer.provideMerge(Logger.minimumLogLevel(LogLevel.All)),
	),
})

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
		const url = new URL(request.url)

		if (request.method === "POST" && url.pathname === "/rpc") {
			// Create a stub to open a communication channel with the Durable Object
			// instance named "foo".
			//
			// Requests from all Workers to the Durable Object instance named "foo"
			// will go to a single remote Durable Object instance.
			const stub = env.MY_DURABLE_OBJECT.getByName("foo");

			// Call the `sayHello()` RPC method on the stub to invoke the method on
			// the remote Durable Object instance.
			// const greeting = await stub.sayHello("world");

			const response = await handler(request)
			console.log(response)
		}

		return new Response("Not found", { status: 404 })
	},
} satisfies ExportedHandler<Env>;

