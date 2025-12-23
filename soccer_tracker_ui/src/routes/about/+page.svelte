<script>
	import { Alert, AlertDescription } from '$lib/components/ui/alert';
	import { Badge } from '$lib/components/ui/badge';
	import { Card, CardContent } from '$lib/components/ui/card';
	import { Separator } from '$lib/components/ui/separator';
</script>

<svelte:head>
	<title>About | Football Statistics Tracker</title>
	<meta name="description" content="About the Football Statistics Tracker project" />
</svelte:head>

<div class="container mx-auto max-w-4xl px-4 py-10">
	<div class="mb-8 flex justify-center">
		<img src="/images/soccerDE_logo.png" width="225" alt="Football Statistics Tracker Logo" />
	</div>

	<Alert class="mb-8 border-amber-200 bg-amber-50">
		<AlertDescription>
			<span class="font-bold">⚠️ LEARNING PROJECT:</span> This is a personal learning project in the
			field of data engineering. I understand the architecture might not be the most optimal as this
			project. I made this to practise and learn. Feedback and suggestions are highly welcomed!
		</AlertDescription>
	</Alert>

	<h1 class="mb-4 text-4xl font-bold">Football Statistics Tracker 📊⚽</h1>

	<div class="mb-8 flex flex-wrap gap-2">
		<Badge variant="outline" class="bg-green-100">Python 3.12</Badge>
		<Badge variant="outline" class="bg-blue-100">GCP Powered</Badge>
		<Badge variant="outline" class="bg-purple-100">IaC Terraform</Badge>
		<Badge variant="outline" class="bg-orange-100">Svelte</Badge>
		<Badge variant="outline" class="bg-blue-100">BigQuery</Badge>
		<Badge variant="outline" class="bg-yellow-100">Firestore</Badge>
	</div>

	<p class="mb-6">
		An end-to-end data engineering pipeline that collects, processes, and analyzes football match
		results, standings data, weather data, Reddit data and summarizes matchdays using Gemini from
		the top 5 European leagues. Used data sources include <a
			href="https://www.football-data.org/"
			class="text-blue-600 hover:underline">football-data.org</a
		>
		API,
		<a href="https://open-meteo.com/" class="text-blue-600 hover:underline">Open-Meteo</a> API, and
		<a href="https://praw.readthedocs.io/en/stable/" class="text-blue-600 hover:underline">PRAW</a>
		(Reddit API),
		<a href="https://developers.google.com/maps" class="text-blue-600 hover:underline">Maps</a>...
	</p>

	<Separator class="my-6" />

	<h2 class="mb-4 text-2xl font-bold">Introduction</h2>
	<p class="mb-6">
		This project demonstrates a complete data pipeline for football (soccer) results, from data
		extraction to visualization. It implements some data engineering practices including data lakes,
		transformation layers, and Infrastructure as Code (IaC) with Terraform.
	</p>

	<h2 class="mb-4 text-2xl font-bold">Features</h2>
	<ul class="mb-6 list-disc space-y-2 pl-6">
		<li>
			<span class="font-semibold">Automated Data Collection:</span> Scheduled data fetching from multiple
			APIs using Google Cloud Functions
		</li>
		<li>
			<span class="font-semibold">Multi-layer Data Architecture:</span> Raw data stored in GCS, processed
			data in BigQuery, and user-facing data in Firestore
		</li>
		<li>
			<span class="font-semibold">Weather Integration:</span> Match statistics with weather data at match
			time
		</li>
		<li>
			<span class="font-semibold">Social Media (Reddit) Data:</span> Reddit comments for fan sentiment
		</li>
		<li>
			<span class="font-semibold">Infrastructure as Code:</span> Cloud Functions and Pub/Sub subscriptions
			and topics defined and deployed with Terraform
		</li>
	</ul>

	<h2 class="mb-4 text-2xl font-bold">Architecture</h2>
	<Card class="mb-6">
		<CardContent class="p-4">
			<img src="/images/architecture.png" alt="Architecture Diagram" class="w-full" />
		</CardContent>
	</Card>

	<p class="mb-4">The pipeline follows the following architecture:</p>
	<ol class="mb-6 list-decimal space-y-2 pl-6">
		<li>
			<span class="font-semibold">Data Ingestion:</span> Cloud Functions trigger on schedule to fetch
			data
		</li>
		<li>
			<span class="font-semibold">Storage Layers:</span> Raw data(json) → External BQ tables (Parquet)
			→ Processed Data in BQ → Firestore
		</li>
		<li>
			<span class="font-semibold">Validation:</span> Very simple validation and Data quality with Dataplex
		</li>
		<li>
			<span class="font-semibold">Summarization:</span> Creation of short summaries in Markdown with
			Gemini
		</li>
		<li>
			<span class="font-semibold">Visualization:</span> This web app for insights
		</li>
	</ol>

	<h2 class="mb-4 text-2xl font-bold">Data Sources</h2>
	<ul class="mb-6 list-disc space-y-2 pl-6">
		<li>
			<span class="font-semibold">Football-data.org:</span> Match data, team data, and standings
		</li>
		<li><span class="font-semibold">Open-Meteo API:</span> Historical weather data</li>
		<li><span class="font-semibold">Reddit (via PRAW):</span> Fan comments and sentiment</li>
		<li><span class="font-semibold">Maps SDK:</span> Location of stadiums</li>
	</ul>

	<h2 class="mb-4 text-2xl font-bold">Technology Stack</h2>
	<div class="mb-6 overflow-x-auto">
		<table class="w-full border-collapse">
			<thead>
				<tr class="border-b">
					<th class="px-4 py-2 text-left">Category</th>
					<th class="px-4 py-2 text-left">Technologies</th>
				</tr>
			</thead>
			<tbody>
				<tr class="border-b">
					<td class="px-4 py-2">Cloud Platform</td>
					<td class="px-4 py-2">Google Cloud Platform (GCP)</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Infrastructure as Code</td>
					<td class="px-4 py-2">Terraform</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Programming Languages</td>
					<td class="px-4 py-2">Python, TypeScript (Svelte)</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Data Storage</td>
					<td class="px-4 py-2">Cloud Storage, BigQuery, Firestore</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Data Quality</td>
					<td class="px-4 py-2">Dataplex</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Data Transformation</td>
					<td class="px-4 py-2">Dataform</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Serverless Computing</td>
					<td class="px-4 py-2">Cloud Functions</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Event-Driven Architecture</td>
					<td class="px-4 py-2">Pub/Sub</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">API Consumption</td>
					<td class="px-4 py-2">Football-data.org, Open-Meteo, Reddit API, Google Maps</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">CI/CD</td>
					<td class="px-4 py-2">GitHub Actions</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Package Management</td>
					<td class="px-4 py-2">uv, pyproject.toml</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Code Quality</td>
					<td class="px-4 py-2">Ruff, Bandit, Mypy</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Testing</td>
					<td class="px-4 py-2">pytest</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Web Framework</td>
					<td class="px-4 py-2">Svelte, ShadCN UI Components</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">Hosting</td>
					<td class="px-4 py-2">Firebase App Hosting</td>
				</tr>
				<tr class="border-b">
					<td class="px-4 py-2">LLM</td>
					<td class="px-4 py-2">Google Gemini 2.5 Flash</td>
				</tr>
			</tbody>
		</table>
	</div>

	<Alert class="mb-8 border-amber-200 bg-amber-50">
		<AlertDescription>
			<span class="font-bold">⚠️ DISCLAIMER:</span> I know this data probably does not have much real
			value as it is not real-time and the statistics are not that deep ( I wanted to stay within free
			tiers of APIs).
		</AlertDescription>
	</Alert>

	<Separator class="my-6" />

	<p class="mb-8 text-sm text-gray-600">
		I got the idea to make this project from this
		<a
			href="https://github.com/digitalghost-dev/premier-league"
			class="text-blue-600 hover:underline">repo</a
		>
		by
		<a href="https://github.com/digitalghost-dev" class="text-blue-600 hover:underline"
			>digitalghost-dev</a
		>
	</p>
</div>
