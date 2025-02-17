<script lang="ts">
	import '../app.css';
	import { Button } from '$lib/components/ui/button';
	import { Github } from 'lucide-svelte';
	let { children } = $props();
</script>

<div class="bg-background min-h-screen">
	<header class="bg-card border-b">
		<div class="container mx-auto p-4">
			<nav class="flex items-center justify-between">
				<div class="flex items-center gap-6">
					<Button variant="ghost" size="lg" href="/" class="font-bold">⚽ Home</Button>
					<Button variant="ghost" href="/matches">Match Data</Button>
				</div>
				<div>
					<Button variant="ghost" size="icon" href="https://github.com/peter115342" target="_blank">
						<Github class="h-5 w-5" />
						<span class="sr-only">GitHub</span>
					</Button>
				</div>
			</nav>
		</div>
	</header>

	<main class="container mx-auto p-4">
		{@render children()}
	</main>
</div>
