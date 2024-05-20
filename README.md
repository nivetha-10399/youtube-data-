# youtube-data

**Introduction**

YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a MongoDB database, subsequently migrated to a SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.


**Table of Contents**

1. Key Technologies and Skills
2. Installation
3. Usage
4. Features


**Key Technologies and Skills**
- Python
- SQL
- MongoDB
- Pandas
- Application Programming Interface (API)
- Streamlit




**Installation**

To run this project, you need to install the following packages:
```python
pip install google-api-python-client
pip install pymongo
pip install pandas
pip install psycopg2
pip install streamlit


**Features**

**Data Collection:** Utilize the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments. By interacting with the Google API, we collect the data and merge it into a JSON file.

**Storing Data into MongoDB:** The retrieved data is stored in a MongoDB database based on user authorization. If the data already exists in the database, it can be overwritten with user consent. This storage process ensures efficient data management and preservation, allowing for seamless handling of the collected data.

**Migrating Data to SQL:** The application allows users to migrate data from MongoDB to a SQL data warehouse. Users can choose which channel's data to migrate. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing SQL queries.

**Data Analysis and Visualization:** The project provides comprehensive data analysis capabilities using Plotly and Streamlit. With the integrated Plotly library, users can create interactive and visually appealing charts and graphs to gain insights from the collected data.

- **Channel Analysis:** Channel analysis includes insights on playlists, videos, subscribers, views, likes, comments, and durations. Gain a deep understanding of the channel's performance and audience engagement through detailed visualizations and summaries.

- **Video Analysis:** Video analysis focuses on views, likes, comments, and durations, enabling both an overall channel and specific channel perspectives. Leverage visual representations and metrics to extract valuable insights from individual videos.

Utilizing the power of Plotly, users can create various types of charts, including line charts, bar charts, scatter plots, pie charts, and more. These visualizations enhance the understanding of the data and make it easier to identify patterns, trends, and correlations. The Streamlit app provides an intuitive interface to interact with the charts and explore the data visually. 

Users can customize the visualizations, filter data, and zoom in or out to focus on specific aspects of the analysis. With the combined capabilities of Plotly and Streamlit, the Data Analysis section empowers users to uncover valuable insights and make data-driven decisions.
