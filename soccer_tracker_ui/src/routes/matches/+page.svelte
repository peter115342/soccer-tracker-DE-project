<script lang="ts">
	import { onMount } from 'svelte';
	import { getLocalTimeZone, today, type CalendarDate } from '@internationalized/date';
	import { Card, CardContent, CardHeader, CardTitle } from '$lib/components/ui/card';
	import { Calendar } from '$lib/components/ui/calendar';
	import { matches, fetchMatchesByDate } from '$lib/stores/matches';
	import { Skeleton } from '$lib/components/ui/skeleton';

	let selectedDate: CalendarDate = today(getLocalTimeZone());
	let loading = true;

	function toJSDate(date: CalendarDate): Date {
		return new Date(date.year, date.month - 1, date.day);
	}

	async function handleDateChange(date: CalendarDate) {
		loading = true;
		if (date) {
			const jsDate = toJSDate(date);
			await fetchMatchesByDate(jsDate);
		}
		loading = false;
	}

	onMount(async () => {
		await handleDateChange(selectedDate);
	});

	$: if (selectedDate) {
		handleDateChange(selectedDate);
	}
</script>

<div class="container mx-auto p-4">
	<div class="grid gap-8 md:grid-cols-[400px_1fr]">
		<Card>
			<CardHeader>
				<CardTitle>Select Date</CardTitle>
			</CardHeader>
			<CardContent>
				<Calendar type="single" bind:value={selectedDate} class="rounded-md border" />
			</CardContent>
		</Card>
		<div class="space-y-4">
			<h2 class="text-2xl font-bold">
				Matches for
				{selectedDate
					? toJSDate(selectedDate).toLocaleDateString('en-US', {
							weekday: 'long',
							year: 'numeric',
							month: 'long',
							day: 'numeric'
						})
					: ''}
			</h2>

			{#if loading}
				{#each Array(5) as _}
					<Card>
						<CardContent class="p-6">
							<div class="flex items-center justify-between">
								<div class="space-y-2 text-center">
									<Skeleton class="mx-auto h-12 w-12 rounded-full" />
									<Skeleton class="mx-auto h-4 w-24" />
									<Skeleton class="mx-auto h-8 w-8" />
								</div>

								<div class="space-y-2 text-center">
									<Skeleton class="mx-auto h-4 w-16" />
									<Skeleton class="mx-auto h-4 w-32" />
									<Skeleton class="mx-auto h-4 w-20" />
								</div>

								<div class="space-y-2 text-center">
									<Skeleton class="mx-auto h-12 w-12 rounded-full" />
									<Skeleton class="mx-auto h-4 w-24" />
									<Skeleton class="mx-auto h-8 w-8" />
								</div>
							</div>
						</CardContent>
					</Card>
				{/each}
			{:else}
				{#each $matches as match}
					<Card>
						<CardContent class="p-6">
							<div class="flex items-center justify-between">
								<div class="space-y-2 text-center">
									<img src={match.home_team_logo} alt={match.home_team} class="mx-auto h-12 w-12" />
									<p class="font-semibold">{match.home_team}</p>
									<p class="text-2xl font-bold">{match.home_score ?? '-'}</p>
								</div>

								<div class="space-y-2 text-center">
									<p class="text-muted-foreground text-sm">{match.status}</p>
									<p class="text-sm">{match.league.name}</p>
									{#if match.weather}
										<p class="text-sm">🌡️ {match.weather.temperature}°C</p>
									{/if}
								</div>

								<div class="space-y-2 text-center">
									<img src={match.away_team_logo} alt={match.away_team} class="mx-auto h-12 w-12" />
									<p class="font-semibold">{match.away_team}</p>
									<p class="text-2xl font-bold">{match.away_score ?? '-'}</p>
								</div>
							</div>
						</CardContent>
					</Card>
				{/each}
			{/if}
		</div>
	</div>
</div>
