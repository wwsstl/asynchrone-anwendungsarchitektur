package de.wwsstl.asynchrone.cloud;

import com.fasterxml.jackson.annotation.JsonProperty;

import tools.jackson.databind.JsonNode;

/**
 * Eine Datei aus der {@code inbox}, wie sie an Cloud-API 1 übermittelt wird.
 *
 * @param fileName Name der Datei; im JSON das Feld {@code identifikator}, über das die Antwort der Datei zugeordnet
 *                 wird
 * @param content  Inhalt der Datei, in JSON umgewandelt; er wird als JSON-Wert übermittelt, nicht als Zeichenkette
 */
public record TestdataItem(@JsonProperty("identifikator") String fileName, JsonNode content) {
}
