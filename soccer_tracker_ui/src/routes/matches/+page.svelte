<script lang="ts">
	import { onMount } from 'svelte';
	import { getLocalTimeZone, parseDate, type DateValue } from '@internationalized/date';
	import { Card, CardContent, CardHeader, CardTitle } from '$lib/components/ui/card';
	import { Calendar } from '$lib/components/ui/calendar';
	import {
		Collapsible,
		CollapsibleContent,
		CollapsibleTrigger
	} from '$lib/components/ui/collapsible';
	import { Button } from '$lib/components/ui/button';
	import { ChevronDown } from 'lucide-svelte';
	import { marked } from 'marked';
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
	import { matchSummary, fetchMatchSummary } from '$lib/stores/summaries';
	import { Skeleton } from '$lib/components/ui/skeleton';
	import { Select, SelectContent, SelectItem, SelectTrigger } from '$lib/components/ui/select';
	import { getWeatherInfo } from '$lib/weather_data';

	let selectedDate: DateValue;
	let loading = true;

	function toJSDate(date: DateValue): Date {
		return new Date(date.year, date.month - 1, date.day);
	}

	function formatDateToString(date: DateValue): string {
		const year = String(date.year);
		const month = String(date.month).padStart(2, '0');
		const day = String(date.day).padStart(2, '0');
		return `${year}-${month}-${day}`;
	}

	function isDateUnavailable(date: DateValue) {
		const dateStr = formatDateToString(date);
		return !$availableDates.has(dateStr);
	}

	async function handleDateChange(date: DateValue) {
		if (!$selectedLeague) return;
		loading = true;
		if (date) {
			const dateStr = formatDateToString(date);
			await Promise.all([
				fetchMatchesByDateAndLeague(toJSDate(date), $selectedLeague.id),
				fetchMatchSummary(dateStr, $selectedLeague.name)
			]);
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
			const dateStr = $latestMatchDate.toISOString().split('T')[0];
			selectedDate = parseDate(dateStr);
			await handleDateChange(selectedDate);
		}
	}

	onMount(async () => {
		await fetchAvailableLeagues();
	});

	$: if ($latestMatchDate && !selectedDate) {
		const dateStr = $latestMatchDate.toISOString().split('T')[0];
		selectedDate = parseDate(dateStr);
	}

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

			{#if $matchSummary}
				<Collapsible class="mb-8 w-full">
					<div
						class="flex items-center justify-between space-x-4 rounded-t-lg bg-slate-100 px-4 py-2"
					>
						<h3 class="text-2xl font-semibold text-slate-800">Match Day Summary</h3>
						<CollapsibleTrigger>
							<Button variant="ghost" size="sm" class="hover:bg-slate-200">
								<ChevronDown class="h-6 w-6" />
								<span class="sr-only">Toggle summary</span>
							</Button>
						</CollapsibleTrigger>
					</div>
					<CollapsibleContent class="space-y-2">
						<Card class="border-2 border-slate-200">
							<CardContent
								class="prose prose-lg prose-pre:whitespace-pre-wrap prose-headings:font-bold prose-headings:mt-6 prose-headings:mb-2 prose-p:mb-4 prose-h1:text-2xl prose-h2:text-xl max-w-none bg-white p-8"
							>
								<p>{$matchSummary}</p>
							</CardContent>
						</Card>
					</CollapsibleContent>
				</Collapsible>
			{/if}

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
											<p class="text-2xl">{getWeatherInfo(match.weather.weathercode).icon}</p>
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
