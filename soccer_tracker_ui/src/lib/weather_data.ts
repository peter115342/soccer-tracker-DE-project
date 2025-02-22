export const weatherData: { [key: number]: { icon: string; description: string } } = {
	0: { icon: '☀️', description: 'Clear sky, cloud development not observed' },
	1: { icon: '🌤️', description: 'Mainly clear, clouds dissolving' },
	2: { icon: '⛅', description: 'Partly cloudy, state of sky unchanged' },
	3: { icon: '☁️', description: 'Overcast, clouds forming' },
	4: { icon: '🌫️', description: 'Visibility reduced by smoke' },
	5: { icon: '🌫️', description: 'Haze' },
	6: { icon: '🌫️', description: 'Widespread dust' },
	7: { icon: '🌫️', description: 'Dust or sand raised by wind' },
	8: { icon: '🌪️', description: 'Dust/sand whirls' },
	9: { icon: '🌪️', description: 'Dust/sandstorm' },
	10: { icon: '🌫️', description: 'Mist' },
	11: { icon: '🌫️', description: 'Shallow fog patches' },
	12: { icon: '🌫️', description: 'Continuous shallow fog' },
	13: { icon: '⚡', description: 'Lightning no thunder' },
	14: { icon: '🌧️', description: 'Precipitation within sight, not reaching ground' },
	15: { icon: '🌧️', description: 'Distant precipitation' },
	16: { icon: '🌧️', description: 'Nearby precipitation' },
	17: { icon: '⛈️', description: 'Thunderstorm without precipitation' },
	18: { icon: '💨', description: 'Squalls' },
	19: { icon: '🌪️', description: 'Funnel clouds' },
	20: { icon: '🌧️', description: 'Drizzle or snow grains' },
	21: { icon: '🌧️', description: 'Recent rain' },
	22: { icon: '🌨️', description: 'Recent snow' },
	23: { icon: '🌨️', description: 'Recent rain and snow' },
	24: { icon: '🌧️', description: 'Recent freezing precipitation' },
	25: { icon: '🌧️', description: 'Recent rain showers' },
	26: { icon: '🌨️', description: 'Recent snow showers' },
	27: { icon: '🌧️', description: 'Recent hail showers' },
	28: { icon: '🌫️', description: 'Recent fog' },
	29: { icon: '⛈️', description: 'Recent thunderstorm' },
	30: { icon: '🌪️', description: 'Slight/moderate duststorm, decreasing' },
	31: { icon: '🌪️', description: 'Slight/moderate duststorm, no change' },
	32: { icon: '🌪️', description: 'Slight/moderate duststorm, increasing' },
	33: { icon: '🌪️', description: 'Severe duststorm, decreasing' },
	34: { icon: '🌪️', description: 'Severe duststorm, no change' },
	35: { icon: '🌪️', description: 'Severe duststorm, increasing' },
	36: { icon: '🌨️', description: 'Slight/moderate blowing snow' },
	37: { icon: '🌨️', description: 'Heavy drifting snow' },
	38: { icon: '🌨️', description: 'Slight/moderate blowing snow' },
	39: { icon: '🌨️', description: 'Heavy drifting snow' },
	40: { icon: '🌫️', description: 'Distant fog' },
	41: { icon: '🌫️', description: 'Fog patches' },
	42: { icon: '🌫️', description: 'Fog, sky visible, thinning' },
	43: { icon: '🌫️', description: 'Fog, sky invisible, thinning' },
	44: { icon: '🌫️', description: 'Fog, sky visible, no change' },
	45: { icon: '🌫️', description: 'Fog, sky invisible, no change' },
	46: { icon: '🌫️', description: 'Fog, sky visible, thickening' },
	47: { icon: '🌫️', description: 'Fog, sky invisible, thickening' },
	48: { icon: '🌫️', description: 'Fog, depositing rime, sky visible' },
	49: { icon: '🌫️', description: 'Fog, depositing rime, sky invisible' },
	50: { icon: '🌧️', description: 'Intermittent light drizzle' },
	51: { icon: '🌧️', description: 'Continuous light drizzle' },
	52: { icon: '🌧️', description: 'Intermittent moderate drizzle' },
	53: { icon: '🌧️', description: 'Continuous moderate drizzle' },
	54: { icon: '🌧️', description: 'Intermittent heavy drizzle' },
	55: { icon: '🌧️', description: 'Continuous heavy drizzle' },
	56: { icon: '🌧️', description: 'Light freezing drizzle' },
	57: { icon: '🌧️', description: 'Moderate/heavy freezing drizzle' },
	58: { icon: '🌧️', description: 'Light drizzle and rain' },
	59: { icon: '🌧️', description: 'Moderate/heavy drizzle and rain' },
	60: { icon: '🌧️', description: 'Intermittent light rain' },
	61: { icon: '🌧️', description: 'Continuous light rain' },
	62: { icon: '🌧️', description: 'Intermittent moderate rain' },
	63: { icon: '🌧️', description: 'Continuous moderate rain' },
	64: { icon: '🌧️', description: 'Intermittent heavy rain' },
	65: { icon: '🌧️', description: 'Continuous heavy rain' },
	66: { icon: '🌧️', description: 'Light freezing rain' },
	67: { icon: '🌧️', description: 'Moderate/heavy freezing rain' },
	68: { icon: '🌨️', description: 'Light rain and snow' },
	69: { icon: '🌨️', description: 'Moderate/heavy rain and snow' },
	70: { icon: '🌨️', description: 'Intermittent light snow' },
	71: { icon: '🌨️', description: 'Continuous light snow' },
	72: { icon: '🌨️', description: 'Intermittent moderate snow' },
	73: { icon: '🌨️', description: 'Continuous moderate snow' },
	74: { icon: '🌨️', description: 'Intermittent heavy snow' },
	75: { icon: '🌨️', description: 'Continuous heavy snow' },
	76: { icon: '🌨️', description: 'Diamond dust' },
	77: { icon: '🌨️', description: 'Snow grains' },
	78: { icon: '🌨️', description: 'Snow crystals' },
	79: { icon: '🌨️', description: 'Ice pellets' },
	80: { icon: '🌧️', description: 'Light rain showers' },
	81: { icon: '🌧️', description: 'Moderate/heavy rain showers' },
	82: { icon: '🌧️', description: 'Violent rain showers' },
	83: { icon: '🌨️', description: 'Light snow/rain showers' },
	84: { icon: '🌨️', description: 'Moderate/heavy snow/rain showers' },
	85: { icon: '🌨️', description: 'Light snow showers' },
	86: { icon: '🌨️', description: 'Moderate/heavy snow showers' },
	87: { icon: '🌨️', description: 'Light snow/ice pellet showers' },
	88: { icon: '🌨️', description: 'Moderate/heavy snow/ice pellet showers' },
	89: { icon: '🌧️', description: 'Light hail showers' },
	90: { icon: '🌧️', description: 'Moderate/heavy hail showers' },
	91: { icon: '⛈️', description: 'Thunderstorm with light rain' },
	92: { icon: '⛈️', description: 'Thunderstorm with moderate/heavy rain' },
	93: { icon: '⛈️', description: 'Thunderstorm with light snow/rain' },
	94: { icon: '⛈️', description: 'Thunderstorm with moderate/heavy snow/rain' },
	95: { icon: '⛈️', description: 'Thunderstorm with light hail' },
	96: { icon: '⛈️', description: 'Thunderstorm with moderate/heavy hail' },
	97: { icon: '⛈️', description: 'Thunderstorm with rain and dust' },
	98: { icon: '⛈️', description: 'Thunderstorm with heavy dust' },
	99: { icon: '⛈️', description: 'Thunderstorm with heavy hail' }
};

export function getWeatherInfo(code: number): { icon: string; description: string } {
	return weatherData[code] || { icon: '❓', description: 'Unknown weather condition' };
}
