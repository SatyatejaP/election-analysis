import simplejson as json
import time

import pandas as pd
import streamlit as st
from matplotlib import pyplot as plt
import numpy as np
from streamlit_autorefresh import st_autorefresh
from confluent_kafka import Consumer
from utils import consume_messages, createKafkaTopic, kafka_cnf

hash_val = hash(time.time())

if 'lead_candidate_per_constituency_df' not in st.session_state:
    st.session_state['lead_candidate_per_constituency_df'] = pd.DataFrame()

if 'votes_per_candidate_df' not in st.session_state:
    st.session_state['votes_per_candidate_df'] = pd.DataFrame()

if 'top_parties_per_constituency_df' not in st.session_state:
    st.session_state['top_parties_per_constituency_df'] = pd.DataFrame()
if 'party_vote_share_df' not in st.session_state:
    st.session_state['party_vote_share_df'] = pd.DataFrame()


def create_consumer(auto_offset_reset='earliest'):
    print(f'Creating consumer with auto_offset_reset={auto_offset_reset}')
    conf = {
        'bootstrap.servers': kafka_cnf['bootstrap_servers'],  # replace with your bootstrap servers
        'group.id': 'my_consumer_group{}'.format(hash_val),
        'auto.offset.reset': auto_offset_reset,
    }

    consumer = Consumer(conf)

    return consumer


class Visualizer:

    def __init__(self):
        createKafkaTopic(kafka_cnf['aggregated_votes_topic'])
        self.last_refreshed = None
        self.updateCounter = 0
        print("Visualizer initialized")

    def fetch_votes_per_candidate(self):
        topic = kafka_cnf['aggregated_votes_topic']
        print(f"fetch aggregated votes topic: {topic}")
        cons = create_consumer(auto_offset_reset='earliest' if self.updateCounter == 0 else 'latest')
        curr_data = consume_messages(consumer=cons, topic=topic)
        df = pd.DataFrame(curr_data)
        cons.close()
        return df

    def updatePage(self):
        print("updating page...")
        # Placeholder to display last refresh time
        last_refresh = st.empty()
        self.last_refreshed = time.strftime('%Y-%m-%d %H:%M:%S')
        last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        curr_votes_per_candidate = self.fetch_votes_per_candidate()
        prev_votes_per_candidate = st.session_state['votes_per_candidate_df']
        print(f'length of curr batch votes {len(curr_votes_per_candidate)}')
        if not curr_votes_per_candidate.empty:
            # update votes per candidate
            print("Updating Votes per Candidate info...")
            union_votes_per_candidate = pd.concat([prev_votes_per_candidate, curr_votes_per_candidate])
            highest_count_per_candidate_idx = union_votes_per_candidate.groupby(['candidate_id'])['vote_count'].idxmax()
            union_votes_per_candidate2 = union_votes_per_candidate.loc[
                highest_count_per_candidate_idx].drop_duplicates()
            # print(union_votes_per_candidate)
            st.session_state['votes_per_candidate_df'] = union_votes_per_candidate2
            print("Updated Votes per Candidate info.")

            # update leads per constituency
            print("Updating Lead Candidate per Constituency info...")
            highest_count_per_constituency_idx = curr_votes_per_candidate.groupby(['constituency_id'])[
                'vote_count'].idxmax()
            curr_lead_per_constituency = curr_votes_per_candidate.loc[highest_count_per_constituency_idx].filter(
                ['constituency_name', 'candidate_name', 'party_name', 'vote_count'], axis=1)
            # print(curr_lead_per_constituency)
            st.session_state['lead_candidate_per_constituency_df'] = curr_lead_per_constituency.sort_values(
                by='constituency_name')
            print("Updated Lead Candidate per Constituency info")

            # Group by 'party_name' and get the size of each group
            parties_leading_counts = st.session_state['votes_per_candidate_df'].groupby('party_name').size()

            # Get the top parties based on the number of constituencies they are leading in
            top_parties_per_constituency = parties_leading_counts.nlargest()
            top_parties_per_constituency_df = top_parties_per_constituency.reset_index()
            top_parties_per_constituency_df.columns = ['party_name', 'leading_constituencies_count']
            top_parties_per_constituency_df.sort_values('leading_constituencies_count', ascending=False)
            st.session_state['top_parties_per_constituency_df'] = top_parties_per_constituency_df
            print("Updated Top Parties per constituency info")


            #Get top parties based on vote share in each constituency
            party_vote_share_df = st.session_state['votes_per_candidate_df'].groupby('party_name')['vote_count'].sum().reset_index()
            party_vote_share_df.columns = ['party_name', 'vote_share']
            party_vote_share_df.sort_values('vote_share', ascending=False)
            st.session_state['party_vote_share_df'] = party_vote_share_df
            print("Updated Party Vote Share info")

            self.updateCounter += 1


if 'viz' not in st.session_state:
    st.session_state['viz'] = Visualizer()

st.title('Rajya Sabha Election Dashboard')
st.markdown("""---""")
tab1, tab2 = st.tabs(["Overview", "Results"])

st.session_state['viz'].updatePage()

if not st.session_state.get('votes_per_candidate_df').empty:
    # Tab1
    tab1.header("Statistcs")

    leading_party = st.session_state['votes_per_candidate_df'].groupby('party_name')['vote_count'].sum().idxmax()
    tab1.subheader(f"Leading Party : {leading_party}")


    tab1.subheader("Top 3 Parties")
    top3parties = st.session_state['top_parties_per_constituency_df'].nlargest(3, 'leading_constituencies_count')
    top3parties.columns = ['Party Name', 'Leading Constituencies Count']
    tab1.table(top3parties.set_index('Party Name'))



    tab1.subheader("Party wise vote share")
    # party wise vote share as pie chart
    labels = st.session_state['party_vote_share_df']['party_name']
    sizes = st.session_state['party_vote_share_df']['vote_share']
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%',
            shadow=False, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    tab1.pyplot(fig1)
    # tab1.bar_chart(st.session_state['party_vote_share_df'].set_index('party_name'))

    # leads = st.session_state['top_parties_df'].filter(['party_name', 'leading_constituencies_count'])
    # tab1.table(leads.reset_index(drop=True))

    # Tab2
    tab2.header("Constituency wise Results")
    selected_state = tab2.selectbox("Select State",
                                    st.session_state['votes_per_candidate_df'][
                                        'state'].unique())
    st.session_state['state_selected'] = selected_state

    constituencies = st.session_state['votes_per_candidate_df'][
        st.session_state['votes_per_candidate_df']['state'] == st.session_state['state_selected']][
        'constituency_name'].unique()
    selected_constituency = tab2.selectbox("Select Constituency",
                                           constituencies)
    st.session_state['constituency_selected'] = selected_constituency

    filtered_df = st.session_state['votes_per_candidate_df'][
        ((st.session_state['constituency_selected'] is None) & (
                    st.session_state['votes_per_candidate_df']['state'] == st.session_state['state_selected'])) | (
                st.session_state['constituency_selected'] == st.session_state['votes_per_candidate_df'][
            'constituency_name'])].filter(
        ['constituency_name', 'candidate_name', 'party_name', 'vote_count'], axis=1).sort_values(by='vote_count',
                                                                                                 ascending=False).reset_index(
        drop=True)

    tab2.table(filtered_df)

if st.session_state.get('last_update') is None:
    st.session_state['last_update'] = time.time()
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 5)
st_autorefresh(interval=refresh_interval * 1000, key="auto")

# Button to manually refresh resources
# if st.sidebar.button('Refresh Data'):
#     viz.updatePage()


# if __name__ == '__main__':
#     viz = Visualizer()
#     viz.updatePage()
