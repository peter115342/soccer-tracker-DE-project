<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { doc, getDoc } from 'firebase/firestore';
	import { db } from '$lib/firebase';
	import { Card, CardContent, CardHeader, CardTitle } from '$lib/components/ui/card';
	import { Tabs, TabsContent, TabsList, TabsTrigger } from '$lib/components/ui/tabs';
	import { Button } from '$lib/components/ui/button';
	import { Skeleton } from '$lib/components/ui/skeleton';
	import 'leaflet/dist/leaflet.css';
	import { getWeatherInfo } from '$lib/weather_data';

	export let data;
	const matchId = data.id;
	let match: any = null;
	let map: any;
	let loading = true;
	let activeTab = 'weather';

	function handleTabChange(value: string) {
		activeTab = value;
	}

	onMount(async () => {
		const matchRef = doc(db, 'matches', matchId);
		const matchDoc = await getDoc(matchRef);
		if (matchDoc.exists()) {
			match = matchDoc.data();
			if (typeof window !== 'undefined' && match.location) {
				await new Promise((resolve) => setTimeout(resolve, 100));

				const L = await import('leaflet');

				// Fix Leaflet's default icon paths with type assertion to avoid TypeScript error
				delete (L.Icon.Default.prototype as any)._getIconUrl;
				L.Icon.Default.mergeOptions({
					iconRetinaUrl: 'https://unpkg.com/leaflet@1.7.1/dist/images/marker-icon-2x.png',
					iconUrl: 'https://unpkg.com/leaflet@1.7.1/dist/images/marker-icon.png',
					shadowUrl: 'https://unpkg.com/leaflet@1.7.1/dist/images/marker-shadow.png'
				});

				const mapContainer = document.getElementById('standalone-map');
				if (mapContainer) {
					map = L.map('standalone-map').setView([match.location.lat, match.location.lon], 15);

					L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
						maxZoom: 19,
						attribution: '© OpenStreetMap contributors'
					}).addTo(map);

					L.marker([match.location.lat, match.location.lon]).addTo(map);
				}
			}
		}
		loading = false;
	});

	onDestroy(() => {
		if (map) {
			map.remove();
			map = null;
		}
	});
</script>

<div class="container mx-auto max-w-4xl p-4">
	<div class="mb-4">
		<Button variant="outline" href="/matches">← Back to Matches</Button>
	</div>

	{#if match}
		<Card class="mb-6">
			<CardHeader class="space-y-2">
				<CardTitle class="text-center text-lg sm:text-xl md:text-2xl">
					{match.home_team} vs {match.away_team}
				</CardTitle>
				<p class="text-muted-foreground text-center text-xs sm:text-sm">Status: {match.status}</p>
			</CardHeader>
			<CardContent>
				<div class="grid grid-cols-1 items-center gap-8 sm:grid-cols-3">
					<div class="flex flex-col items-center space-y-4">
						<img
							src={match.home_team_logo}
							alt={match.home_team}
							class="h-20 w-20 sm:h-24 sm:w-24"
						/>
						<h3 class="text-center text-base font-bold">{match.home_team}</h3>
						<p class="text-2xl font-bold">{match.home_score ?? '-'}</p>
					</div>

					<div class="order-3 flex flex-col items-center space-y-4 py-4 sm:order-2">
						<p class="text-sm">{new Date(match.date).toLocaleDateString()}</p>
						<div class="flex items-center gap-2">
							<img src={match.league.logo} alt={match.league.name} class="h-6 w-6" />
							<span class="text-sm">{match.league.name}</span>
						</div>
						<p class="text-muted-foreground text-center text-xs">
							Last updated: {new Date(match.last_updated).toLocaleString()}
						</p>
					</div>

					<div class="order-2 flex flex-col items-center space-y-4 sm:order-3">
						<img
							src={match.away_team_logo}
							alt={match.away_team}
							class="h-20 w-20 sm:h-24 sm:w-24"
						/>
						<h3 class="text-center text-base font-bold">{match.away_team}</h3>
						<p class="text-2xl font-bold">{match.away_score ?? '-'}</p>
					</div>
				</div>

				{#if match.location}
					<div class="mt-8 sm:mt-2">
						<div
							id="standalone-map"
							class="h-[200px] w-full rounded-lg border sm:h-[250px] md:h-[300px]"
						></div>
						<p class="text-center text-sm">{match.venue}</p>
					</div>
				{/if}
			</CardContent>
		</Card>

		{#if loading}
			<Card>
				<CardHeader>
					<Skeleton class="h-8 w-[200px]" />
				</CardHeader>
				<CardContent>
					<div class="grid grid-cols-1 gap-4 sm:grid-cols-2 md:grid-cols-4">
						{#each Array(7) as _}
							<div class="flex flex-col items-center rounded-lg border p-4">
								<Skeleton class="h-6 w-[100px]" />
								<Skeleton class="my-2 h-8 w-[80px]" />
							</div>
						{/each}
					</div>
				</CardContent>
			</Card>
		{:else}
			<Tabs value={activeTab} onValueChange={handleTabChange} class="space-y-4">
				<TabsList class="grid w-full grid-cols-2 gap-4">
					<TabsTrigger value="weather">Weather</TabsTrigger>
					<TabsTrigger value="reddit">Reddit Discussion</TabsTrigger>
				</TabsList>

				<TabsContent value="weather">
					<Card>
						<CardHeader>
							<CardTitle>Match Weather Conditions</CardTitle>
						</CardHeader>
						<CardContent>
							<div class="grid grid-cols-1 gap-4 sm:grid-cols-2 md:grid-cols-4">
								<div class="flex flex-col items-center rounded-lg border p-4">
									<span class="text-sm">Weather Condition</span>
									<span class="my-2 text-3xl">{getWeatherInfo(match.weather.weathercode).icon}</span
									>
									<span class="text-center text-xs"
										>{getWeatherInfo(match.weather.weathercode).description}</span
									>
								</div>
								<div class="flex flex-col items-center rounded-lg border p-4">
									<span class="text-sm">Temperature</span>
									<span class="mt-2 text-xl font-bold">{match.weather.temperature}°C</span>
								</div>
								<div class="flex flex-col items-center rounded-lg border p-4">
									<span class="text-sm">Apparent Temperature</span>
									<span class="mt-2 text-xl font-bold">{match.weather.apparent_temperature}°C</span>
								</div>
								<div class="flex flex-col items-center rounded-lg border p-4">
									<span class="text-sm">Wind Speed</span>
									<span class="mt-2 text-xl font-bold">{match.weather.wind_speed} km/h</span>
								</div>
								<div class="flex flex-col items-center rounded-lg border p-4">
									<span class="text-sm">Humidity</span>
									<span class="mt-2 text-xl font-bold">{match.weather.humidity}%</span>
								</div>
								<div class="flex flex-col items-center rounded-lg border p-4">
									<span class="text-sm">Cloud Cover</span>
									<span class="mt-2 text-xl font-bold">{match.weather.cloud_cover}%</span>
								</div>
								<div class="flex flex-col items-center rounded-lg border p-4">
									<span class="text-sm">Precipitation</span>
									<span class="mt-2 text-xl font-bold">{match.weather.precipitation} mm</span>
								</div>
							</div>
						</CardContent>
					</Card>
				</TabsContent>

				<TabsContent value="reddit">
					<Card>
						<CardHeader>
							<CardTitle class="text-base sm:text-lg">
								Reddit Discussions at the end of {new Date(match.date).toLocaleDateString()}
							</CardTitle>
						</CardHeader>
						<CardContent>
							{#if match.reddit_data?.threads}
								{#each match.reddit_data.threads as thread}
									<div class="border-b py-4">
										<div class="flex gap-4 text-sm">
											<p>Score: {thread.score}</p>
											<p>Comments: {thread.num_comments}</p>
										</div>
										<div class="mt-4 space-y-3">
											{#each thread.comments as comment}
												<div class="bg-muted rounded-lg p-4">
													<p class="mb-2 text-sm font-semibold">u/{comment.author}</p>
													<p class="mb-2 text-sm">{comment.body}</p>
													<p class="text-muted-foreground text-xs">Score: {comment.score}</p>
												</div>
											{/each}
										</div>
									</div>
								{/each}
							{:else}
								<p class="py-4 text-sm">No Reddit discussions available</p>
							{/if}
						</CardContent>
					</Card>
				</TabsContent>
			</Tabs>
		{/if}
	{:else}
		<Card>
			<CardContent>
				<p class="py-8 text-center">Match not found</p>
			</CardContent>
		</Card>
	{/if}
</div>
