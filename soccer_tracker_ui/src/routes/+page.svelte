<script lang="ts">
	import { onMount } from 'svelte';
	import { upcomingMatches, fetchUpcomingMatches } from '$lib/stores/upcoming_matches';
	import { Card, CardContent, CardHeader, CardTitle } from '$lib/components/ui/card';
	import { Separator } from '$lib/components/ui/separator';
	import { Skeleton } from '$lib/components/ui/skeleton';

	let loading = true;

	onMount(async () => {
		await fetchUpcomingMatches();
		loading = false;
	});
</script>

<div class="container mx-auto p-4">
	{#if loading}
		<Skeleton class="mb-6 h-12 w-[500px]" />
		<Skeleton class="mb-6 h-6 w-[400px]" />

		{#each Array(3) as _}
			<Card class="mb-6">
				<CardHeader>
					<Skeleton class="h-8 w-[300px]" />
				</CardHeader>
				<CardContent>
					<div class="space-y-4">
						{#each Array(3) as _}
							<div class="flex items-center justify-between p-4">
								<div class="flex flex-1 items-center justify-end gap-2">
									<Skeleton class="h-6 w-[120px]" />
									<Skeleton class="h-8 w-8 rounded-full" />
								</div>
								<div class="px-4">
									<Skeleton class="h-6 w-[80px]" />
								</div>
								<div class="flex flex-1 items-center gap-2">
									<Skeleton class="h-8 w-8 rounded-full" />
									<Skeleton class="h-6 w-[120px]" />
								</div>
							</div>
							<div class="flex items-center justify-center">
								<Skeleton class="h-6 w-[200px]" />
							</div>
							{#if _ !== 2}
								<Separator class="my-2" />
							{/if}
						{/each}
					</div>
				</CardContent>
			</Card>
		{/each}
	{:else}
		<h1 class="mb-6 text-3xl font-bold">
			Matches for {new Date($upcomingMatches[0]?.date).toLocaleDateString('en-US', {
				weekday: 'long',
				year: 'numeric',
				month: 'long',
				day: 'numeric'
			})}
		</h1>
		<p class="text-muted-foreground mb-6">Match data will be available at 1AM UTC the next day</p>

		{#each $upcomingMatches as dayMatches}
			<Card class="mb-6">
				<CardHeader>
					<CardTitle>
						{new Date(dayMatches.date).toLocaleDateString('en-US', {
							weekday: 'long',
							year: 'numeric',
							month: 'long',
							day: 'numeric'
						})}
					</CardTitle>
				</CardHeader>
				<CardContent>
					<div class="space-y-4">
						{#each dayMatches.matches as match}
							<div
								class="bg-card hover:bg-accent flex items-center justify-between rounded-lg p-4 transition-colors"
							>
								<div class="flex flex-1 items-center justify-end gap-2 text-right">
									<span class="font-semibold">{match.home_team}</span>
									<img
										src={match.home_team_crest}
										alt={match.home_team}
										class="h-8 w-8 object-contain"
									/>
								</div>

								<div class="flex items-center space-x-2 px-4">
									<span class="bg-primary/10 rounded-full px-3 py-1 text-sm">
										{new Date(match.kickoff).toLocaleTimeString('en-US', {
											hour: '2-digit',
											minute: '2-digit'
										})}
									</span>
								</div>

								<div class="flex flex-1 items-center gap-2 text-left">
									<img
										src={match.away_team_crest}
										alt={match.away_team}
										class="h-8 w-8 object-contain"
									/>
									<span class="font-semibold">{match.away_team}</span>
								</div>
							</div>

							<div
								class="text-muted-foreground flex items-center justify-center gap-2 text-center text-sm"
							>
								<span>{match.competition_name}</span>
								{#if match.area_flag}
									<img src={match.area_flag} alt={match.area_name} class="h-6 w-6 object-contain" />
								{/if}
							</div>

							{#if match !== dayMatches.matches[dayMatches.matches.length - 1]}
								<Separator class="my-2" />
							{/if}
						{/each}
					</div>
				</CardContent>
			</Card>
		{/each}
	{/if}
</div>
