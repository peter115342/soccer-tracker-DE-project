<script lang="ts">
	import { onMount } from 'svelte';
	import { upcomingMatches, fetchUpcomingMatches } from '$lib/stores/upcoming_matches';
	import { standings, fetchStandings } from '$lib/stores/standings';
	import { Card, CardContent, CardHeader, CardTitle } from '$lib/components/ui/card';
	import { Separator } from '$lib/components/ui/separator';
	import { Skeleton } from '$lib/components/ui/skeleton';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table';
	import { Tabs, TabsContent, TabsList, TabsTrigger } from '$lib/components/ui/tabs';

	let loading = true;

	onMount(async () => {
		await Promise.all([fetchUpcomingMatches(), fetchStandings()]);
		loading = false;
	});
</script>

<div class="grid grid-cols-1 gap-6">
	<!-- Matches Column -->
	<div>
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
		{:else if $upcomingMatches.length === 0}
			<Card class="mb-6">
				<CardHeader>
					<CardTitle>
						No Matches for {new Date().toLocaleDateString('en-US', {
							year: 'numeric',
							month: 'long',
							day: 'numeric'
						})}
					</CardTitle>
				</CardHeader>
				<CardContent>
					<p class="text-muted-foreground">
						There are no matches scheduled for {new Date().toLocaleDateString('en-US', {
							year: 'numeric',
							month: 'long',
							day: 'numeric'
						})}
					</p>
				</CardContent>
			</Card>
		{:else}
			<div class="mb-6">
				<h1 class="mb-4 text-3xl font-bold">
					Matches for {new Date($upcomingMatches[0]?.date).toLocaleDateString('en-US', {
						weekday: 'long',
						year: 'numeric',
						month: 'long',
						day: 'numeric'
					})}
				</h1>

				<p class="text-muted-foreground mb-6">
					Match data will be available at 1AM UTC the next day
				</p>

				<ScrollArea class="h-[calc(100vh-12rem)] w-full rounded-md border">
					<div class="p-4">
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
												class="bg-card hover:bg-accent flex flex-col items-center justify-between gap-4 rounded-lg p-4 transition-colors sm:flex-row"
											>
												<div class="flex flex-1 items-center justify-end gap-2 text-right">
													<span class="font-semibold">{match.home_team}</span>
													<img
														src={match.home_team_crest}
														alt={match.home_team}
														class="h-8 w-8 object-contain"
													/>
												</div>

												<div class="flex items-center space-x-2">
													<span
														class="bg-primary/10 whitespace-nowrap rounded-full px-3 py-1 text-sm"
													>
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
													<img
														src={match.area_flag}
														alt={match.area_name}
														class="h-6 w-6 object-contain"
													/>
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
					</div>
				</ScrollArea>
			</div>
		{/if}
	</div>

	<!-- Standings Column -->
	<div>
		{#if loading}
			<Card>
				<CardHeader>
					<Skeleton class="h-8 w-[200px]" />
				</CardHeader>
				<CardContent>
					<Skeleton class="h-[400px] w-full" />
				</CardContent>
			</Card>
		{:else}
			<Card>
				<CardHeader>
					<CardTitle>League Standings</CardTitle>
				</CardHeader>
				<CardContent>
					<Tabs value={$standings[0]?.competition_id.toString()}>
						<TabsList class="grid h-20 w-full grid-cols-5">
							{#each $standings as league}
								<TabsTrigger value={league.competition_id.toString()} class="py-2">
									{#if league.competition_id === 2014}
										<div class="flex flex-col items-center gap-2">
											<img
												src="https://crests.football-data.org/760.svg"
												alt="La Liga"
												class="h-8 w-8"
											/>
											<span class="whitespace-nowrap text-xs">La Liga</span>
										</div>
									{:else if league.competition_id === 2002}
										<div class="flex flex-col items-center gap-2">
											<img
												src="https://crests.football-data.org/759.svg"
												alt="Bundesliga"
												class="h-8 w-8"
											/>
											<span class="whitespace-nowrap text-xs">Bundesliga</span>
										</div>
									{:else if league.competition_id === 2019}
										<div class="flex flex-col items-center gap-2">
											<img
												src="https://crests.football-data.org/784.svg"
												alt="Serie A"
												class="h-8 w-8"
											/>
											<span class="whitespace-nowrap text-xs">Serie A</span>
										</div>
									{:else if league.competition_id === 2021}
										<div class="flex flex-col items-center gap-2">
											<img
												src="https://crests.football-data.org/770.svg"
												alt="Premier League"
												class="h-8 w-8"
											/>
											<span class="whitespace-nowrap text-xs">Premier League</span>
										</div>
									{:else if league.competition_id === 2015}
										<div class="flex flex-col items-center gap-2">
											<img
												src="https://crests.football-data.org/773.svg"
												alt="Ligue 1"
												class="h-8 w-8"
											/>
											<span class="whitespace-nowrap text-xs">Ligue 1</span>
										</div>
									{/if}
								</TabsTrigger>
							{/each}
						</TabsList>

						{#each $standings as league}
							<TabsContent value={league.competition_id.toString()}>
								<Table>
									<TableHeader>
										<TableRow>
											<TableHead class="w-12">Pos</TableHead>
											<TableHead>Team</TableHead>
											<TableHead class="text-right">MP</TableHead>
											<TableHead class="text-right">W</TableHead>
											<TableHead class="text-right">D</TableHead>
											<TableHead class="text-right">L</TableHead>
											<TableHead class="text-right">GF</TableHead>
											<TableHead class="text-right">GA</TableHead>
											<TableHead class="text-right">GD</TableHead>
											<TableHead class="text-right">Pts</TableHead>
										</TableRow>
									</TableHeader>
									<TableBody>
										{#each league.table as team}
											<TableRow>
												<TableCell>{team.position}</TableCell>
												<TableCell class="flex items-center gap-2">
													<img src={team.team_crest} alt={team.team_name} class="h-4 w-4" />
													<div class="flex flex-col">
														<span>{team.team_name}</span>
														<span class="text-muted-foreground text-xs">{team.team_tla}</span>
													</div>
												</TableCell>
												<TableCell class="text-right">{team.played_games}</TableCell>
												<TableCell class="text-right">{team.won}</TableCell>
												<TableCell class="text-right">{team.draw}</TableCell>
												<TableCell class="text-right">{team.lost}</TableCell>
												<TableCell class="text-right">{team.goals_for}</TableCell>
												<TableCell class="text-right">{team.goals_against}</TableCell>
												<TableCell class="text-right">{team.goal_difference}</TableCell>
												<TableCell class="text-right font-bold">{team.points}</TableCell>
											</TableRow>
										{/each}
									</TableBody>
								</Table>
							</TabsContent>
						{/each}
					</Tabs>
				</CardContent>
			</Card>
		{/if}
	</div>
</div>
