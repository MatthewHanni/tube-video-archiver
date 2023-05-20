# tube-video-archiver
Personal project; working proof of concept to scan a user's own YouTube playlists and make a copy of those videos in S3.

This repository contains a working proof of concept for a program that scans a user's YouTube playlists and copies the videos to Amazon S3. The program is dockerized and deployed to an AWS Lambda function, which is triggered every few hours by EventBridge.

When the program starts, it connects to the user's YouTube account via the API and retrieves a list of their playlists and the videos in each playlist. The program then checks each video ID to see if it is already stored in S3. If the video is not stored in S3, the program checks to see if it has tried to download the video before and encountered an exception. If the program has encountered more than a certain number of exceptions for a video, it stops trying to download the video and moves on. Some exceptions are due to random errors that may resolve on a second or third try, while other exceptions require manual override and will never be able to be downloaded. The program does not want to incur costs or waste time by continuing to attempt to download these videos.

Once a video is successfully downloaded, the program exits. The Lambda function will continue to execute every few hours, so the program will eventually download all videos in the user's playlists, even if a significant number of videos are added during a short timeframe. Of course, prolific YouTube users may set the EventBridge triggers more frequently.

This program can be used to back up a user's YouTube playlists, or to download videos for offline viewing. It can also be used to create a personal video library that can be accessed from anywhere.