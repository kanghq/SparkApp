/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gis.spatial.rtree;

import org.neo4j.logging.Logger;
import java.io.PrintStream;

/**
 * This listener logs percentage progress to the specified PrintStream or Logger based on a timer,
 * never logging more frequently than the specified number of ms.
 */
public class ProgressLoggingListener implements Listener {

    private final ProgressLog out;
    private final String name;
    private long lastLogTime = 0L;
    private int totalUnits = 0;
    private int workedSoFar = 0;
    private boolean enabled = false;
    private long timeWait = 1000;

    public interface ProgressLog {
        void log(String line);
    }

    public ProgressLoggingListener(String name, final PrintStream out) {
        this.name = name;
        this.out = line -> out.println(line);
    }

    public ProgressLoggingListener(String name, Logger logger) {
        this.name = name;
        this.out = line -> logger.log(line);
    }

    public ProgressLoggingListener setTimeWait(long ms) {
        this.timeWait = ms;
        return this;
    }

	@Override
    public void begin(int unitsOfWork) {
        this.totalUnits = unitsOfWork;
        this.workedSoFar = 0;
        this.lastLogTime = 0L;
        try {
            this.enabled = true;
            out.log("Starting " + name);
        } catch (Exception e) {
            System.err.println("Failed to write to output - disabling progress logger: " + e.getMessage());
            this.enabled = false;
        }
    }
	
	@Override
    public void worked(int workedSinceLastNotification) {
        this.workedSoFar += workedSinceLastNotification;
        logNoMoreThanOnceASecond("Running");
    }
	
	@Override
    public void done() {
        this.workedSoFar = this.totalUnits;
        this.lastLogTime = 0L;
        logNoMoreThanOnceASecond("Completed");
    }

    private void logNoMoreThanOnceASecond(String action) {
        long now = System.currentTimeMillis();
        if (enabled && now - lastLogTime > timeWait) {
            if (totalUnits > 0) {
                out.log("" + percText() + " (" + workedSoFar + "/" + totalUnits + ") - " + action + " " + name);
            } else {
                out.log(action + " " + name);
            }
            this.lastLogTime = now;
        }
    }

    private String percText() {
        if (totalUnits > 0) {
            return String.format("%.2f", 100.0 * workedSoFar / totalUnits);
        } else {
            return "NaN";
        }
    }
}
