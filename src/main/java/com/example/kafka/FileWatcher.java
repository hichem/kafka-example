package com.example.kafka;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchService;


import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;

public class FileWatcher extends Thread {


	//Member Variables
	private WatchService 								watcher				= null;
	private boolean										keepRunning			= true;
	private FileWatcherListener					listener			= null;
	private Path										watchedDir			= null;
	private String										watchedFile			= null;


	public FileWatcherListener getListener() {
		return listener;
	}

	public void setListener(FileWatcherListener listener) {
		this.listener = listener;
	}

	public Path getWatchedDir() {
		return watchedDir;
	}

	public void setWatchedDir(Path watchedDir) {
		this.watchedDir = watchedDir;
	}

	public String getWatchedFile() {
		return watchedFile;
	}

	public void setWatchedFile(String watchedFile) {
		this.watchedFile = watchedFile;
	}

	public FileWatcher() {
		
	}
	
	public FileWatcher(Path aWatchedDir, String aWatchedFile) {

		//Initialize watcher
		try {
			watcher = FileSystems.getDefault().newWatchService();

			watchedDir = aWatchedDir;
			watchedDir.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
			watchedFile = aWatchedFile;

		} catch (IOException e) {
			MyLogger.getSharedLogger().logError(String.format("[FileWatcher::FileWatcher] Failed to initialize watcher. Error: %s", e.getMessage()));
		}

	}




	@Override
	public void run() {

		//Start watch service polling loop
		WatchKey key;

		while (keepRunning) {

			try {
				// wait for a key to be available
				key = watcher.take();
			} catch (InterruptedException e) {
				MyLogger.getSharedLogger().logError(String.format("[FileWatcher::FileWatcher] Watch Service Thread has been interrupted. Error: %s", e.getMessage()));
				return;
			}

			for (WatchEvent<?> event : key.pollEvents()) {
				// get event type
				WatchEvent.Kind<?> kind = event.kind();

				// get file name
				@SuppressWarnings("unchecked")
				WatchEvent<Path> ev = (WatchEvent<Path>) event;
				Path fileName = ev.context();

				if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {

					if(fileName.equals(Paths.get(watchedFile))) {

						//Reload configuration
						GlobalConfig.getInstance().loadConfiguration();

						//Notify Listener
						listener.onConfigurationChanged();
					}
				}
				/*
		        else if (kind == StandardWatchEventKinds.OVERFLOW) {

		        } else if (kind == StandardWatchEventKinds.ENTRY_CREATE) {

		        } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {

		        }
				 */
			}

			// IMPORTANT: The key must be reset after processed
			boolean valid = key.reset();
			if (!valid) {
				MyLogger.getSharedLogger().logWarn("[ConfigFileWatcher::run] Failed to reset the watch key");
			}
		}


		super.run();
	}

}
