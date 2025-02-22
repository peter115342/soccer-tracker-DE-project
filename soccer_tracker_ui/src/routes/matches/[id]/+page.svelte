<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { doc, getDoc } from 'firebase/firestore';
	import { db } from '$lib/firebase';
	import { Card, CardContent, CardHeader, CardTitle } from '$lib/components/ui/card';
	import { Tabs, TabsContent, TabsList, TabsTrigger } from '$lib/components/ui/tabs';
	import { Skeleton } from '$lib/components/ui/skeleton';
	import 'ol/ol.css';
	import { getWeatherInfo } from '$lib/weather_data';

	export let data;
	const matchId = data.id;
	let match: any = null;
	let loading = true;
	let map: any;
	let activeTab = 'weather';

	function handleTabChange(value: string) {
		if (value === 'map' && map) {
			setTimeout(() => {
				map.updateSize();
			}, 100);
		}
	}

	onMount(async () => {
		const matchRef = doc(db, 'matches', matchId);
		const matchDoc = await getDoc(matchRef);
		if (matchDoc.exists()) {
			match = matchDoc.data();

			if (typeof window !== 'undefined' && match.location) {
				const { Map, View } = await import('ol');
				const TileLayer = await import('ol/layer/Tile').then((m) => m.default);
				const OSM = await import('ol/source/OSM').then((m) => m.default);
				const { fromLonLat } = await import('ol/proj');
				const { Point } = await import('ol/geom');
				const Feature = await import('ol/Feature.js').then((m) => m.default);
				const VectorLayer = await import('ol/layer/Vector').then((m) => m.default);
				const VectorSource = await import('ol/source/Vector').then((m) => m.default);
				const { Style, Icon } = await import('ol/style');

				const stadiumLocation = fromLonLat([match.location.lon, match.location.lat]);

				map = new Map({
					target: 'standalone-map',
					layers: [
						new TileLayer({
							source: new OSM()
						})
					],
					view: new View({
						center: stadiumLocation,
						zoom: 15,
						minZoom: 2,
						maxZoom: 19
					})
				});

				const marker = new Feature({
					geometry: new Point(stadiumLocation)
				});

				const vectorSource = new VectorSource({
					features: [marker]
				});

				const vectorLayer = new VectorLayer({
					source: vectorSource,
					style: new Style({
						image: new Icon({
							anchor: [0.5, 1],
							scale: 0.4,
							src: 'https://openlayers.org/en/latest/examples/data/icon.png'
						})
					})
				});

				map.addLayer(vectorLayer);

				window.setTimeout(() => {
					map.updateSize();
					map.renderSync();
				}, 200);
			}
		}
		loading = false;
	});

	onDestroy(() => {
		if (map) {
			map.setTarget(null);
			map = null;
		}
	});
</script>

<div class="container mx-auto space-y-6 p-4">
	{#if loading}
		<Skeleton class="h-[200px] w-full" />
	{:else if match}
		<Card>
			<CardHeader>
				<CardTitle class="text-center text-2xl">
					{match.home_team} vs {match.away_team}
				</CardTitle>
				<p class="text-muted-foreground text-center text-sm">Status: {match.status}</p>
			</CardHeader>
			<CardContent>
				<div class="grid grid-cols-3 items-center gap-8">
					<div class="flex flex-col items-center space-y-4">
						<img src={match.home_team_logo} alt={match.home_team} class="h-24 w-24" />
						<h3 class="text-xl font-bold">{match.home_team}</h3>
						<p class="text-3xl font-bold">{match.home_score ?? '-'}</p>
					</div>

					<div class="flex flex-col items-center justify-center space-y-2">
						<p class="text-lg">{new Date(match.date).toLocaleDateString()}</p>
						<p class="text-sm">{match.venue}</p>
						<div class="flex items-center gap-2">
							<img src={match.league.logo} alt={match.league.name} class="h-6 w-6" />
							<span>{match.league.name}</span>
						</div>
						<p class="text-muted-foreground text-xs">
							Last updated: {new Date(match.last_updated).toLocaleString()}
						</p>
					</div>

					<div class="flex flex-col items-center space-y-4">
						<img src={match.away_team_logo} alt={match.away_team} class="h-24 w-24" />
						<h3 class="text-xl font-bold">{match.away_team}</h3>
						<p class="text-3xl font-bold">{match.away_score ?? '-'}</p>
					</div>
				</div>
			</CardContent>
		</Card>

		<Tabs value={activeTab} onValueChange={handleTabChange}>
			<TabsList class="grid w-full grid-cols-3">
				<TabsTrigger value="weather">Weather</TabsTrigger>
				<TabsTrigger value="reddit">Reddit Discussion</TabsTrigger>
				<TabsTrigger value="map">Stadium Location</TabsTrigger>
			</TabsList>

			<TabsContent value="weather">
				<Card>
					<CardHeader>
						<CardTitle>Match Weather Conditions</CardTitle>
					</CardHeader>
					<CardContent>
						<div class="grid grid-cols-2 gap-4 md:grid-cols-4">
							<div class="flex flex-col items-center rounded-lg border p-4">
								<span class="text-sm">Weather Condition</span>
								<span class="my-2 text-4xl">{getWeatherInfo(match.weather.weathercode).icon}</span>
								<span class="text-center text-sm"
									>{getWeatherInfo(match.weather.weathercode).description}</span
								>
							</div>
							<div class="flex flex-col items-center rounded-lg border p-4">
								<span class="text-sm">Temperature</span>
								<span class="text-xl font-bold">{match.weather.temperature}°C</span>
							</div>
							<div class="flex flex-col items-center rounded-lg border p-4">
								<span class="text-sm">Apparent Temperature</span>
								<span class="text-xl font-bold">{match.weather.apparent_temperature}°C</span>
							</div>
							<div class="flex flex-col items-center rounded-lg border p-4">
								<span class="text-sm">Wind Speed</span>
								<span class="text-xl font-bold">{match.weather.wind_speed} km/h</span>
							</div>
							<div class="flex flex-col items-center rounded-lg border p-4">
								<span class="text-sm">Humidity</span>
								<span class="text-xl font-bold">{match.weather.humidity}%</span>
							</div>
							<div class="flex flex-col items-center rounded-lg border p-4">
								<span class="text-sm">Cloud Cover</span>
								<span class="text-xl font-bold">{match.weather.cloud_cover}%</span>
							</div>
							<div class="flex flex-col items-center rounded-lg border p-4">
								<span class="text-sm">Precipitation</span>
								<span class="text-xl font-bold">{match.weather.precipitation} mm</span>
							</div>
						</div>
					</CardContent>
				</Card>
			</TabsContent>

			<TabsContent value="reddit">
				<Card>
					<CardHeader>
						<CardTitle
							>Reddit Discussions at the end of {new Date(
								match.date
							).toLocaleDateString()}</CardTitle
						>
					</CardHeader>
					<CardContent>
						{#if match.reddit_data?.threads}
							{#each match.reddit_data.threads as thread}
								<div class="border-b py-4">
									<p>Score: {thread.score}</p>
									<p>Comments: {thread.num_comments}</p>
									<div class="mt-4 space-y-2">
										{#each thread.comments as comment}
											<div class="bg-muted rounded p-3">
												<p class="text-sm font-semibold">u/{comment.author}</p>
												<p>{comment.body}</p>
												<p class="text-muted-foreground text-sm">Score: {comment.score}</p>
											</div>
										{/each}
									</div>
								</div>
							{/each}
						{:else}
							<p>No Reddit discussions available</p>
						{/if}
					</CardContent>
				</Card>
			</TabsContent>

			<TabsContent value="map">
				<Card>
					<CardHeader>
						<CardTitle>Stadium Location</CardTitle>
					</CardHeader>
					<CardContent>
						<div class="h-[400px]" id="map"></div>
						<p class="text-muted-foreground mt-2 text-sm">{match.venue}</p>
					</CardContent>
				</Card>
			</TabsContent>
		</Tabs>
	{:else}
		<Card>
			<CardContent>
				<p class="py-8 text-center">Match not found</p>
			</CardContent>
		</Card>
	{/if}
</div>

<!-- Standalone map container -->
{#if match?.location}
	<div class="text-muted-foreground mb-2 text-center text-sm">
		Debug Coordinates: Lat {match.location.lat}, Lon {match.location.lon}
	</div>
{/if}
<div id="standalone-map" style="width: 100%; height: 400px; margin-top: 20px;"></div>
