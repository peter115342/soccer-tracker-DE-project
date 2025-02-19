<script lang="ts">
	import { onMount } from 'svelte';
	import { getLocalTimeZone, today, parseDate, type DateValue } from '@internationalized/date';
	import { Card, CardContent, CardHeader, CardTitle } from '$lib/components/ui/card';
	import { Calendar } from '$lib/components/ui/calendar';
	import {
		matches,
		fetchMatchesByDateAndLeague,
		availableDates,
		latestMatchDate,
		fetchAvailableDatesByLeague,
		fetchAvailableLeagues,
		availableLeagues,
		selectedLeague,
		type League
	} from '$lib/stores/matches';
	import { Skeleton } from '$lib/components/ui/skeleton';
	import { Select, SelectContent, SelectItem, SelectTrigger } from '$lib/components/ui/select';

	let selectedDate: DateValue = today(getLocalTimeZone());
	let loading = true;

	function toJSDate(date: DateValue): Date {
		return new Date(date.year, date.month - 1, date.day);
	}

	function isDateUnavailable(date: DateValue) {
		const dateStr = `${date.year}-${String(date.month).padStart(2, '0')}-${String(
			date.day
		).padStart(2, '0')}`;
		return !$availableDates.has(dateStr);
	}

	async function handleDateChange(date: DateValue) {
		if (!$selectedLeague) return;
		loading = true;
		if (date) {
			const jsDate = toJSDate(date);
			await fetchMatchesByDateAndLeague(jsDate, $selectedLeague.id);
		}
		loading = false;
	}

	async function handleLeagueChange(value: string) {
		const leagueId = parseInt(value, 10);
		const league = $availableLeagues.find((l) => l.id === leagueId);
		if (!league) return;

		selectedLeague.set(league);
		await fetchAvailableDatesByLeague(league.id);

		if ($latestMatchDate) {
			selectedDate = parseDate($latestMatchDate.toISOString().split('T')[0]);
			await handleDateChange(selectedDate);
		}
	}

	onMount(async () => {
		await fetchAvailableLeagues();
	});

	$: if (selectedDate && $selectedLeague) {
		handleDateChange(selectedDate);
	}
</script>

<div class="container mx-auto p-4">
	<div class="grid gap-8 md:grid-cols-[400px_1fr]">
		<div class="space-y-4">
			<Card>
				<CardHeader>
					<CardTitle>Select League</CardTitle>
				</CardHeader>
				<CardContent>
					<Select
						type="single"
						value={$selectedLeague?.id?.toString()}
						onValueChange={handleLeagueChange}
					>
						<SelectTrigger>
							<span>{$selectedLeague?.name || 'Select a league'}</span>
						</SelectTrigger>
						<SelectContent>
							{#each $availableLeagues as league}
								<SelectItem value={league.id.toString()}>
									<div class="flex items-center gap-2">
										<img src={league.logo} alt={league.name} class="h-6 w-6" />
										<span>{league.name}</span>
									</div>
								</SelectItem>
							{/each}
						</SelectContent>
					</Select>
				</CardContent>
			</Card>

			{#if $selectedLeague}
				<Card>
					<CardHeader>
						<CardTitle>Select Date</CardTitle>
					</CardHeader>
					<CardContent>
						<Calendar
							type="single"
							bind:value={selectedDate}
							class="rounded-md border"
							{isDateUnavailable}
						/>
					</CardContent>
				</Card>
			{/if}
		</div>
		<div class="space-y-4">
			<h2 class="text-2xl font-bold">
				{#if $selectedLeague}
					Matches for {$selectedLeague.name} on
					{selectedDate
						? toJSDate(selectedDate).toLocaleDateString('en-US', {
								weekday: 'long',
								year: 'numeric',
								month: 'long',
								day: 'numeric'
							})
						: ''}
				{:else}
					Select a league to view matches
				{/if}
			</h2>

			{#if loading}
				{#each Array(5) as _}
					<Card>
						<CardContent class="p-6">
							<div class="grid grid-cols-3 items-center gap-4">
								<div class="flex flex-col items-center space-y-2">
									<Skeleton class="h-12 w-12 rounded-full" />
									<Skeleton class="h-4 w-24" />
									<Skeleton class="h-8 w-8" />
								</div>
								<div class="flex flex-col items-center justify-center space-y-2">
									<Skeleton class="h-4 w-16" />
									<Skeleton class="h-4 w-32" />
									<Skeleton class="h-4 w-20" />
								</div>
								<div class="flex flex-col items-center space-y-2">
									<Skeleton class="h-12 w-12 rounded-full" />
									<Skeleton class="h-4 w-24" />
									<Skeleton class="h-8 w-8" />
								</div>
							</div>
						</CardContent>
					</Card>
				{/each}
			{:else}
				{#each $matches as match}
					<a href="/matches/{match.id}">
						<Card>
							<CardContent class="p-6">
								<div class="grid grid-cols-3 items-center gap-4">
									<div class="flex flex-col items-center space-y-2">
										<img
											src={match.home_team_logo}
											alt={match.home_team}
											class="h-12 w-12 object-contain"
										/>
										<p class="flex min-h-[2.5rem] items-center text-center font-semibold">
											{match.home_team}
										</p>
										<p class="text-2xl font-bold">{match.home_score ?? '-'}</p>
									</div>

									<div class="flex flex-col items-center justify-center space-y-2">
										{#if match.weather}
											<p class="text-sm">🌡️ {match.weather.temperature}°C</p>
										{/if}
									</div>

									<div class="flex flex-col items-center space-y-2">
										<img
											src={match.away_team_logo}
											alt={match.away_team}
											class="h-12 w-12 object-contain"
										/>
										<p class="flex min-h-[2.5rem] items-center text-center font-semibold">
											{match.away_team}
										</p>
										<p class="text-2xl font-bold">{match.away_score ?? '-'}</p>
									</div>
								</div>
							</CardContent>
						</Card>
					</a>
				{/each}
			{/if}
		</div>
	</div>
</div>
