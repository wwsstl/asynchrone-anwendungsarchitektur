package de.wwsstl.asynchrone.cloud;

/** Eine aus der {@code inbox} gelesene Testdatendatei, wie sie an Cloud-API 1 übergeben wird. */
public record TestdataItem(String fileName, String content) {
}
