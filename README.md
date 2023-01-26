# Saksinfo fra AAP
I dette repoet vil vi sette opp alle api for deling av data på tvers av ytelser i NAV. Under her listes alle api og versjoner. Sjekk versjoner og status der.

## Spørsmål, henvendelser og tilgang
Alle spørsmål og henvendelser kan sendes til slack-kanal `#po-aap-dev`. For å få tilgang til API må du be oss om tilgang.

# Versjoner

## Versjon 1
| API-type | Status dev   | Status prod |
|----------|--------------|-------------|
 | Kafka    | Ikke laget   | Ikke laget  |
 | Rest     | Under arbeid | Ikke laget  |

### Rest-API
Per i dag er det laget et enkelt REST-api for å hente ut data. Per nå så svarer dette kun på om du har et aktivt AAP-vedtak (du går på AAP).

#### Request
```
GET https://aap-saksinfo.dev.intern.nav.no/v1/vedtak  
X-personident <fødselsnummer>
```

#### Response
```json
{
 "harIverksattVedtak": true,
 "kilde": "arena"
}
```