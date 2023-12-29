from datetime import datetime

def formatted_current_time():
  return datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
