
package org.apache.flink.runtime.util;

import org.apache.hadoop.conf.Configuration;

/**
 * Implementacion experimental de HadoopUtils.
 *
 * Objetivo:
 * Proporcionar una configuracion Hadoop independiente
 * para la inicializacion del catalogo Iceberg.
 *
 * Pre-entrega 05 - Data Engineering.
 */
public class HadoopUtils {

    public static Configuration getHadoopConfiguration(
            org.apache.flink.configuration.Configuration flinkConfig) {

        return new Configuration(false);
    }
}
