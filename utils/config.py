# Contains all the configurations needed for the Dash app and overall processes
navbar_logo_url = 'https://www.spi.ox.ac.uk/sites/default/files/styles/mt_image_medium/public/spi/images/article/our_world_in_data.jpg?itok=sXt5oJje'
username = "postgres"
password = "Uchihazenzo1994"
database = "covid_19"
table_name = "covid_19_data"
columns = ["date", "country_code", "location", "geo_id", "total_cases", "total_deaths", "new_cases", "new_deaths"]
geo_ids_url = 'https://www.geonames.org/countries/'
continents = ['World', 'Asia', 'Africa', 'Europe', 'North America', 'South America', 'European Union', 'Oceania']
font = 'Courier New'
env = 'local'