from supabase import create_client, Client

url: str = 'https://zdrgurrkwghrbtzfttmf.supabase.co'
key: str = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpkcmd1cnJrd2docmJ0emZ0dG1mIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDc1NzI1NzYsImV4cCI6MjAyMzE0ODU3Nn0.g8HNHTMNXp_2qiTqbe4_GtzyYZuNd-96X8MqMK5QUSM'
supabase: Client = create_client(url, key)
idlahan=1
dataset_name=f'moisture_before_after_{idlahan}.csv'
with open(dataset_name, 'wb+') as f:
    res = supabase.storage.from_('ml-data-feeds').download(dataset_name)
    f.write(res)