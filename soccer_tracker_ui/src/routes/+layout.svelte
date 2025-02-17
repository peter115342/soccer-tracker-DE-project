<script lang="ts">
	import '../app.css';
	import { Button } from '$lib/components/ui/button';
	let { children } = $props();
</script>

<div class="container mx-auto p-4">
	<nav class="mb-8 flex gap-4">
		<Button variant="link" href="/">Upcoming Matches</Button>
		<Button variant="link" href="/matches">Match Calendar</Button>
	</nav>

	{@render children()}
</div>
