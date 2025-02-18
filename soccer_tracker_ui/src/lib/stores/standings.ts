import { writable } from 'svelte/store';
import { collection, getDocs } from 'firebase/firestore';
import { db } from '$lib/firebase';

interface TeamStanding {
	position: number;
	team_id: string;
	team_name: string;
	team_short_name: string;
	team_tla: string;
	team_crest: string;
	played_games: number;
	points: number;
	won: number;
	draw: number;
	lost: number;
	goals_for: number;
	goals_against: number;
	goal_difference: number;
}

interface LeagueStandings {
	competition_id: number;
	table: TeamStanding[];
	last_updated: string;
	current_matchday: number;
}

export const standings = writable<LeagueStandings[]>([]);

export const fetchStandings = async () => {
	const standingsCollection = collection(db, 'current_standings');
	const snapshot = await getDocs(standingsCollection);
	const standingsData = snapshot.docs.map((doc) => ({
		...doc.data()
	})) as LeagueStandings[];
	standings.set(standingsData);
};
