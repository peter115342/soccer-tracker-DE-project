<script lang="ts">
	import '../app.css';
	import { Button } from '$lib/components/ui/button';
	import { Github } from 'lucide-svelte';
	import { page } from '$app/stores';
	let { children } = $props();
	const currentYear = new Date().getFullYear();
</script>

<div class="bg-background flex min-h-screen flex-col">
	<header class="bg-card border-b">
		<div class="container mx-auto px-2 md:px-4">
			<nav class="flex h-14 items-stretch justify-between">
				<div class="flex items-stretch divide-x divide-gray-200">
					<Button
						variant="ghost"
						href="/"
						class="h-full rounded-none px-3 md:px-4 {$page.url.pathname === '/' ? 'font-bold' : ''}"
					>
						Home
					</Button>
					<Button
						variant="ghost"
						href="/matches"
						class="h-full rounded-none px-3 md:px-4 {$page.url.pathname === '/matches'
							? 'font-bold'
							: ''}"
					>
						Match Data
					</Button>
					<Button
						variant="ghost"
						href="/about"
						class="h-full rounded-none px-3 md:px-4 {$page.url.pathname === '/about'
							? 'font-bold'
							: ''}"
					>
						About
					</Button>
				</div>
				<div class="flex items-center">
					<Button
						variant="ghost"
						size="icon"
						href="https://github.com/peter115342"
						target="_blank"
						class="group relative mx-2 flex aspect-square h-10 w-10 items-center justify-center overflow-hidden rounded-full bg-purple-50 transition-all hover:bg-purple-100 hover:shadow-md"
					>
						<span
							class="absolute inset-0 bg-gradient-to-br from-purple-400/10 to-purple-600/20 opacity-0 transition-opacity group-hover:opacity-100"
						></span>
						<span
							class="absolute inset-0 rounded-full bg-purple-600/20 opacity-0 blur-md group-hover:opacity-70"
						></span>
						<Github
							class="relative z-10 h-7 w-7 text-purple-800 transition-transform group-hover:scale-110"
						/>
						<span class="sr-only">GitHub</span>
					</Button>
				</div>
			</nav>
		</div>
	</header>

	<main class="container mx-auto flex-grow p-4">
		{@render children()}
	</main>

	<footer class="bg-card mt-auto border-t">
		<div class="container mx-auto px-4 py-3">
			<div class="flex items-center justify-between">
				<div class="text-muted-foreground text-sm">© {currentYear} Soccer Tracker</div>
				<div class="flex items-center">
					<a
						href="https://github.com/peter115342"
						target="_blank"
						class="flex items-center text-sm text-purple-800 transition-colors hover:text-purple-600"
					>
						<Github class="mr-1 h-4 w-4" />
						<span>peter115342</span>
					</a>
				</div>
			</div>
		</div>
	</footer>
</div>
