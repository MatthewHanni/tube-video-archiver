# tube-video-archiver
Personal project; working proof of concept to scan a user's own YouTube playlists and make a copy of those videos in S3.

This repository becomes Dockerized and deployed to an AWS Lambda. The Lambda is triggered via eventbridge every few hours. When the program kicks off, it connects to the user's YouTube account via API and retrieves a list of the user's playlists, and a list of all videos saved to each playlist. We cycle through each video ID to check to see if a record for that video is recorded in S3 (a JSON record for each downloaded video is maintained separate from the actual video files.) If the video is NOT downloaded, we check to see if we have attempted to download it before and have encountered exceptions. If we've encounted more than a certain number of exceptions, we stop attempting to download that video and move on. Some exceptions are due to random errors that resolve on a second or third try. Other exceptions require manual override (think age-gating or locale restrictions) and will never be able to be downloaded, so we do not wish to incur cost and waste time by continuing to attempt to download these videos. Once a video is downloaded, the script exits. Because the lambda reexecutes every X minutes, it eventually will cycle through all videos even if a significant number are added during a short timeframe.
