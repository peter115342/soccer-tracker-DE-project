import { writable } from 'svelte/store';
import { collection, query, where, getDocs } from 'firebase/firestore';
import { db } from '$lib/firebase';

interface Weather {
	apparent_temperature: number;
	temperature: number;
	precipitation: number;
	wind_speed: number;
	cloud_cover: number;
	humidity: number;
	weathercode: number;
}

export interface League {
	id: number;
	name: string;
	logo: string;
}

interface Match {
	id: string;
	match_id: number;
	date: string;
	status: string;
	home_team: string;
	home_team_id: number;
	away_team: string;
	away_team_id: number;
	home_team_logo: string;
	away_team_logo: string;
	home_score: number;
	away_score: number;
	league: League;
	weather: Weather;
	venue: string;
	location: {
		lat: number;
		lon: number;
	};
	last_updated: string;
}

export const matches = writable<Match[]>([]);
export const availableDates = writable<Set<string>>(new Set());
export const latestMatchDate = writable<Date | null>(null);
export const selectedLeague = writable<League | null>(null);
export const availableLeagues = writable<League[]>([]);

export const fetchAvailableLeagues = async () => {
	const matchesCollection = collection(db, 'matches');
	const q = query(matchesCollection);

	try {
		const snapshot = await getDocs(q);

		const leagueMap: Record<number, League> = {};

		for (const doc of snapshot.docs) {
			const data = doc.data();
			const league = data.league as League;
			leagueMap[league.id] = league;
		}

		const uniqueLeagueList = Object.values(leagueMap);
		availableLeagues.set(uniqueLeagueList);
	} catch (error) {
		console.error('Error fetching leagues:', error);
		availableLeagues.set([]);
	}
};

export const fetchMatchesByDateAndLeague = async (date: Date, leagueId: number) => {
	const startDate = new Date(date);
	startDate.setHours(0, 0, 0, 0);

	const endDate = new Date(date);
	endDate.setHours(23, 59, 59, 999);
	const matchesCollection = collection(db, 'matches');
	const q = query(
		matchesCollection,
		where('date', '>=', startDate.toISOString()),
		where('date', '<=', endDate.toISOString()),
		where('league.id', '==', leagueId)
	);

	try {
		const snapshot = await getDocs(q);
		const matchList = snapshot.docs.map((doc) => ({
			id: doc.id,
			...doc.data()
		})) as Match[];

		matches.set(matchList);
		console.log('Fetched matches:', matchList);
	} catch (error) {
		console.error('Error fetching matches:', error);
		matches.set([]);
	}
};

export const fetchAvailableDatesByLeague = async (leagueId: number) => {
	const matchesCollection = collection(db, 'matches');
	const q = query(matchesCollection, where('league.id', '==', leagueId));

	try {
		const snapshot = await getDocs(q);
		const dates = new Set(snapshot.docs.map((doc) => doc.data().date.split('T')[0]));
		availableDates.set(dates);

		if (snapshot.docs.length > 0) {
			const sortedDates = [...snapshot.docs].sort(
				(a, b) => new Date(b.data().date).getTime() - new Date(a.data().date).getTime()
			);
			latestMatchDate.set(new Date(sortedDates[0].data().date));
		}
	} catch (error) {
		console.error('Error fetching available dates:', error);
		availableDates.set(new Set());
		latestMatchDate.set(null);
	}
};
