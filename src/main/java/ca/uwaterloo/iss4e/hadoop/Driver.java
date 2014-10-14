package ca.uwaterloo.iss4e.hadoop;

/**
 *
 *  Copyright (c) 2014 Xiufeng Liu ( xiufeng.liu@uwaterloo.ca )
 *
 *  This file is free software: you may copy, redistribute and/or modify it
 *  under the terms of the GNU General Public License version 2
 *  as published by the Free Software Foundation.
 *
 *  This file is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see http://www.gnu.org/licenses.
 */


import ca.uwaterloo.iss4e.hadoop.pointperrow.ThreelMain;
import org.apache.hadoop.util.ProgramDriver;

/**
 * A description of an example program based on its class and a
 * human-readable description.
 */
public class Driver {

    public static void main(String argv[]){
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("ThreelMain", ThreelMain.class,   "Threeline program");
            pgd.driver(argv);
        }
        catch(Throwable e){
            e.printStackTrace();
        }
    }
}
