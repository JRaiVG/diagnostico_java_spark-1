package minsait.ttaa.datio.engine;

import minsait.ttaa.datio.common.naming.PlayerInput;
import minsait.ttaa.datio.common.pathfile.PathFile;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark, PathFile path) {
        this.spark = spark;
        Dataset<Row> df = readInput(path.getInput());

        df.printSchema();

        df = cleanData(df);
        df = exampleWindowFunction(df);
        df = windowFunctionNationalityOverall(df);
        df = addColumnPotentialVsOverall(df);
        df = windowFunctionPlayeCatNationalityOverall(df);
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        // Uncomment when you want write your final output
        write(df, path.getOutput());
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                potential.column(),
                playerCat.column(),
                potentialVsOverall.column(),
                overall.column(),
                heightCm.column(),
                teamPosition.column(),
                catHeightByPosition.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     * @param input
     */
    private Dataset<Row> readInput(String input) {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }

    /**
     * @param df es un conjunto de datos con la informaci�n de jugadores
     * @return agrega al conjunto de datos la columna "player_cat"
     * se realiza por la siguiente regla
     * caracter A si el jugador es de los mejores 10 jugadores de su pa�s
     * caracter B si el jugador es de los mejores 20 jugadores de su pa�s
     * caracter C si el jugador es de los mejores 50 jugadores de su pa�s
     * caracter D para el resto de jugadores
     */
    private Dataset<Row> windowFunctionNationalityOverall(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column())
                .orderBy(overall.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), A)
                .when(rank.$less(20), B)
                .when(rank.$less(50), C)
                .otherwise(D);

        df = df.withColumn(playerCat.getName(), rule);

        return df;
    }

    /**
     * @param df es un conjunto de datos con la informaci�n de jugadores
     * @return agregar al conjunto de datos la columna "potential_vs_overall"
     * se realiza por la siguiente regla
     * la columna `potential` dividida por la columna `overall`
     */
    private Dataset<Row> addColumnPotentialVsOverall(Dataset<Row> df) {

        return df.withColumn(potentialVsOverall.getName(),
                col(potential.getName()).divide(col(overall.getName())));
    }


    /**
     * @param df es un conjunto de datos con la informaci�n de jugadores
     * @return Se realiza un filtro mediante dos coulmnas: la columna "player_cat" y la columna "potential_vs_overall"
     * se realiza por la siguiente regla
     * Si "player_cat" esta en los siguientes valores: **A**, **B**
     * Si "player_cat" es **C** y "potential_vs_overall" es superior a **1.15**
     * Si "player_cat" es **D** y "potential_vs_overall" es superior a **1.25**
     */
    private Dataset<Row> windowFunctionPlayeCatNationalityOverall(Dataset<Row> df) {
        df = df.filter(col(playerCat.getName()).equalTo(A)
                .or(col(playerCat.getName()).equalTo(B))
                .or(col(playerCat.getName()).equalTo(C)
                        .and(col(potentialVsOverall.getName()).$greater(1.15)))
                .or(col(playerCat.getName()).equalTo(D)
                        .and(col(potentialVsOverall.getName()).$greater(1.25)))
        );

        return df;
    }


}
