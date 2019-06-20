/**
 * Created by mmami on 10.10.16.
 */
package bde.sebida;

import bde.sebida.classes.Loader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.logging.LogManager;

public class App
{
    public static void main( String[] args ) throws ClassNotFoundException, IOException
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        Logger.getRootLogger().setLevel(Level.ERROR);

        //Class.forName("parquet.Log");
        LogManager.getLogManager().reset();

        String input_path = args[0];
        String output_path = args[1];
        String master = args[2];
        String dataset_name = "lgd";
        String dataset_URI = "lgd/2016";

        /*if (args.length < 2) {
            System.err.println("Usage: bde.sebida.App <input_file> <output_file> ");
            System.exit(1);
        }*/

        //String input_path = "";
        //String output_path = "";
        //String master = "local[*]";
        //String input_path = "";
        //String input_path = "";


        // Extract schema from and load semantic data
        Loader se = new Loader();
        se.fromSemData(input_path, output_path, master);
    }
}
