import json
import threading
import requests
from lxml import html
from bs4 import BeautifulSoup
import os

# Initialize global variables
data_file_path = 'episodes_data.json'
checkpoint_file = 'detailed_checkpoint.json'

# Load the data file with episode URLs
def load_data():
    try:
        with open(data_file_path, 'r') as data_file:
            return json.load(data_file)
    except json.JSONDecodeError as e:
        print(f"An error occurred while loading JSON data: {e}")
        exit(1)

# Load checkpoint
def load_checkpoint():
    try:
        with open(checkpoint_file, 'r') as cp_file:
            return json.load(cp_file)
    except FileNotFoundError:
        return {'podcast_index': 0, 'episode_index': 0}
    except json.JSONDecodeError as e:
        print(f"An error occurred while loading checkpoint JSON data: {e}")
        return {'podcast_index': 0, 'episode_index': 0}

def save_checkpoint(data):
    with open(checkpoint_file, 'w') as cp_file:
        json.dump(data, cp_file, indent=4)

def save_detailed_data(podcast_name, data):
    podcast_file_path = f"{podcast_name.replace(' ', '_')}_detailed_data.json"
    if os.path.exists(podcast_file_path):
        with open(podcast_file_path, 'r') as podcast_file:
            existing_data = json.load(podcast_file)
    else:
        existing_data = []

    existing_data.append(data)
    with open(podcast_file_path, 'w') as podcast_file:
        json.dump(existing_data, podcast_file, indent=4)

def scrape_episode(podcast_name, episode_url, checkpoint, detailed_data_lock):
    try:
        response = requests.get(episode_url)
        response.raise_for_status()
        page_content = response.content
        soup = BeautifulSoup(page_content, 'html.parser')
        tree = html.fromstring(page_content)
        
        episode_name = soup.select_one("#transcript div:nth-of-type(2) div div div div h1").text.strip()
        episode_date = tree.xpath('//*[@id="transcript"]/div[2]/div/div/div/div/span/text()')[0].strip()
        
        sentences = soup.select(".pod_text")
        episode_text = "\n".join([sentence.text.strip() for sentence in sentences])
        
        episode_data = {
            "Podcast Name": podcast_name,
            "Episode Name": episode_name,
            "Episode Date": episode_date,
            "Episode Url": episode_url,
            "Episode Text": episode_text
        }

        with detailed_data_lock:
            save_detailed_data(podcast_name, episode_data)
        
        # Update checkpoint
        with checkpoint_lock:
            checkpoint['episode_index'] += 1
            save_checkpoint(checkpoint)
                
    except requests.RequestException as e:
        print(f"An error occurred while processing episode at {episode_url}: {e}")

def main():
    global detailed_data_lock, checkpoint_lock
    
    all_data = load_data()
    checkpoint = load_checkpoint()

    detailed_data_lock = threading.Lock()
    checkpoint_lock = threading.Lock()
    
    threads = []
    for podcast_index, (podcast_name, episodes) in enumerate(all_data.items()):
        if podcast_index < checkpoint['podcast_index']:
            continue
        
        for episode_index, episode in enumerate(episodes):
            if podcast_index == checkpoint['podcast_index'] and episode_index < checkpoint['episode_index']:
                continue

            episode_url = episode['episode_url']
            thread = threading.Thread(target=scrape_episode, args=(podcast_name, episode_url, checkpoint, detailed_data_lock))
            threads.append(thread)
            thread.start()

            if len(threads) >= 40:  
                for t in threads:
                    t.join()
                threads = []

        # Update podcast index in checkpoint
        with checkpoint_lock:
            checkpoint['podcast_index'] = podcast_index + 1
            checkpoint['episode_index'] = 0  # Reset episode index for the next podcast
            save_checkpoint(checkpoint)
        print(podcast_index)

    # Ensure all threads have finished
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
