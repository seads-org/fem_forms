import os
import pandas as pd
import json
import streamlit as st
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError

working_in_st=True
if working_in_st:
    s3 = boto3.resource(
        's3',
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
        region_name=st.secrets["AWS_REGION"]
    )
    s3_client = boto3.client(
        's3',
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
        region_name=st.secrets["AWS_REGION"]
    )
else:
    s3 = boto3.resource('s3')
    s3_client = boto3.client('s3')

bucket_name = 'fem-transcripts'
previous_sessions_path = 'forms_transcriptors/previous_sessions/'

def load_json(filename):
    object_name = filename.replace(f"s3://{bucket_name}/", "")
    obj = s3.Object(bucket_name, object_name)
    try:
        data = json.loads(obj.get()['Body'].read().decode('utf-8'))
    except:
        data = {}
    return data

def save_json(data, filename):
    object_name = filename.replace(f"s3://{bucket_name}/", "")
    s3.Object(bucket_name, object_name).put(Body=json.dumps(data, indent=4, ensure_ascii=False))

# Function to generate presigned url
def generate_presigned_url(bucket_name, object_name, expiration=3600):
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name, 'Key': object_name, 'ResponseContentType': 'audio/wav'},
                                                    ExpiresIn=expiration)
    except NoCredentialsError:
        return None
    return response


# Function to get .out file content
def get_s3_file_content(bucket_name, object_name):
    s3 = boto3.client('s3')
    try:
        data = s3.get_object(Bucket=bucket_name, Key=object_name)
        json_data = data['Body'].read()
        return json.loads(json_data)['text']
    except NoCredentialsError:
        return None


def display_form(df, page, json_file, items_per_page):
    start = (page - 1) * items_per_page
    end = start + items_per_page

    data = load_json(json_file)

    for i in range(start, end):
        with st.form(key=f'section_{i}'):
            try:
                row = df.iloc[i]
            except IndexError:
                continue
            st.write(f"Audio {i + 1}: {row['sgm_input_location'].split('/')[-1].replace('.wav', '')}")
            audio_presigned_url = generate_presigned_url(row['sgm_input_location'].split('/')[2], '/'.join(row['sgm_input_location'].split('/')[3:])) if pd.notna(row['sgm_input_location']) else None
            st.markdown(f'Presigned URL:  [LINK]({audio_presigned_url})')
            original_transcript = get_s3_file_content(row['sgm_output_location'].split('/')[2], '/'.join(row['sgm_output_location'].split('/')[3:])) if pd.notna(row['sgm_output_location']) else None
            st.write(f"Original transcript: '{original_transcript}'")

            # Check if entry exists in the JSON file and load the corrected transcript
            corrected_transcript = data[row['sgm_input_location']]["corrected_transcript"] if row['sgm_input_location'] in data else ""

            corrected_transcript = st.text_area(
                f"Corrected transcript for Audio {i + 1}", value=corrected_transcript
            )

            save_button = st.form_submit_button(f"Save corrections for Audio {i + 1}")
            if save_button:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                data[row['sgm_input_location']] = {
                    "original_transcript": original_transcript,
                    "corrected_transcript": corrected_transcript,
                    "timestamp": timestamp,
                }
                save_json(data, json_file)

            st.write("---")

def load_data_from_s3(s3_path):
    bucket_name = s3_path.split('/')[2]
    object_name = '/'.join(s3_path.split('/')[3:])
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=object_name)
    except NoCredentialsError:
        return None
    return pd.read_csv(obj['Body'])

def main():
    st.set_page_config(layout='wide')
    language = st.selectbox('Select a language:', ['','hausa', 'igbo', 'yoruba'])

    if language:
        # if 'df' not in st.session_state:
        if 'language' not in st.session_state or language != st.session_state.language:
            st.session_state.language = language
            st.session_state.df = load_data_from_s3(f's3://fem-transcripts/{language}_async_inference/mapping.csv')
            st.session_state.df['form_title'] = st.session_state.df['doc_full_transcription_location'].apply(lambda x: os.path.splitext(os.path.basename(x))[0])
            st.session_state.df = st.session_state.df.dropna(subset=['sgm_input_location'])
            st.session_state.start_form = False
            st.session_state.transcriptor_name = ""
        n_audios = st.session_state.df.shape[0]
        items_per_page = 5

        form_titles = st.session_state.df['form_title'].unique().tolist()
        selected_form_title = st.selectbox('Select a form title:', form_titles)


        st.write("Please enter your name (and press enter):")
        st.session_state.transcriptor_name = st.text_input("Name")
        st.session_state.transcriptor_name = st.session_state.transcriptor_name.lower().replace(" ", "")

        if st.session_state.transcriptor_name:
            if 'start_form' not in st.session_state:
                st.session_state.start_form = False
            if 'continue_previous_session_button' not in st.session_state:
                st.session_state.continue_previous_session_button = False
            if 'new_session_button' not in st.session_state:
                st.session_state.new_session_button = False

            if not st.session_state.start_form:
                st.session_state.start_form = st.button(f"Click here to start with the correction of the form {selected_form_title}")

            if st.session_state.start_form:
                if not st.session_state.continue_previous_session_button and not st.session_state.new_session_button:
                    st.session_state.continue_previous_session_button = st.button("Continue previous session")
                    st.session_state.new_session_button = st.button("New session")

                    if st.session_state.continue_previous_session_button:
                        previous_sessions_bucket = s3.Bucket(bucket_name)
                        previous_sessions = [obj.key.replace(previous_sessions_path,'') for obj in previous_sessions_bucket.objects.filter(Prefix=previous_sessions_path).all() if selected_form_title in obj.key and st.session_state.transcriptor_name in obj.key]
                        if not previous_sessions:
                            st.write("No sessions found. Please refresh the page and start a New Session.")
                        else:
                            selected_session = st.selectbox("Select a previous session:", previous_sessions)

                            if selected_session:
                                st.session_state.json_file = f"s3://{bucket_name}/{previous_sessions_path}{selected_session}"
                                st.session_state.df = st.session_state.df[st.session_state.df['form_title'] == selected_form_title]
                    elif st.session_state.new_session_button:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        st.session_state.json_file = f"s3://{bucket_name}/{previous_sessions_path}{st.session_state.transcriptor_name}_{timestamp}_{selected_form_title}.json"
                        st.session_state.df = st.session_state.df[st.session_state.df['form_title'] == selected_form_title]

                if 'json_file' in st.session_state:
                    if n_audios > 0:
                        # When display_form is being called in main:
                        page = st.number_input("Page", min_value=1, max_value=-(-n_audios // items_per_page), step=1, value=1)
                        display_form(st.session_state.df, page, st.session_state.json_file, items_per_page)
                    else:
                        st.write("No audio files found.")

if __name__ == "__main__":
    main()