# How to Release

## Prerequisites

- Sign up for a Sonatype account [here](https://issues.sonatype.org/secure/Signup!default.jspa)
- Ask for permissions to push to com.spotify domain like in this [ticket](https://issues.sonatype.org/browse/OSSRH-20689)
- Add Sonatype credentials to `~/.sbt/1.0/credentials.sbt`

```scala
credentials ++= Seq(
Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    "$USERNAME",
    "$PASSWORD"))
```
    
- Create a PGP key, for example on [keybase.io](https://keybase.io/), and [distribute](https://www.gnupg.org/gph/en/manual/x457.html) it to a public keyserver 

## Update documentation and version matrix

- Pick a release name from [here](https://en.wikipedia.org/wiki/List_of_Latin_phrases_%28full%29), [here](https://en.wikipedia.org/wiki/List_of_songs_with_Latin_lyrics), [here](https://harrypotter.fandom.com/wiki/List_of_spells), [here](https://en.wikipedia.org/wiki/List_of_Latin_names_of_cities), [here](https://en.wikipedia.org/wiki/List_of_Latin_names_of_countries), or other interesting sources<sup>*</sup>
- Update the list of release names below
- If the release includes a Beam version bump, update the @ref:[version matrix](../Apache-Beam.md)

## Automatic (CI)

Checkout and update the `main` branch.

```bash
git checkout main

git pull
```

Create and push a new version tag

```bash
git tag -a vX.Y.Z -m "vX.Y.Z"

git push origin vX.Y.Z
```

## Manual 

- Run the slow integration tests with `SLOW=true sbt it:test`
- Run `release skip-tests` in sbt console and follow the instructions
- Go to [oss.sonatype.org](https://oss.sonatype.org/), find the staging repository, "close" and "release"
- When the tag build completes, update release notes with name and change log
- Run `./scripts/make-site.sh` to update documentation

## After successfully published artifacts

- Run @github[scripts/bump_scio.sh](/scripts/bump_scio.sh) to update [homebrew formula](https://github.com/spotify/homebrew-public/blob/master/scio.rb) and `scioVersion` in downstream repos including [scio.g8](https://github.com/spotify/scio.g8), [featran](https://github.com/spotify/featran), etc.
- Send external announcement to scio-users@googlegroups.com and user@beam.apache.org
- Announce on public [Slack](https://slackin.spotify.com/)
- Announce on Twitter

<sup>*</sup>Starting with `0.4.0` all release names are scientific names of animals with genus and species starting with the same letter, in ascending alphabetical order; Harry Potter spells starting with `0.8.0`; Latin names of cities in ascending alphabetical order starting `0.10.0`; Latin names of countries in ascending alphabetical order starting `0.11.0`.

## Past release names

### 0.11.x

- [v0.11.1](https://github.com/spotify/scio/releases/tag/v0.11.1) - _"Armorica"_
- [v0.11.0](https://github.com/spotify/scio/releases/tag/v0.11.0) - _"Ariana"_

### 0.10.x

- [v0.10.4](https://github.com/spotify/scio/releases/tag/v0.10.4) - _"Edessa"_
- [v0.10.3](https://github.com/spotify/scio/releases/tag/v0.10.3) - _"Dallasium"_
- [v0.10.2](https://github.com/spotify/scio/releases/tag/v0.10.2) - _"Cantabrigia"_
- [v0.10.1](https://github.com/spotify/scio/releases/tag/v0.10.1) - _"Belli Horizontis"_
- [v0.10.0](https://github.com/spotify/scio/releases/tag/v0.10.0) - _"Aquae Sextiae"_

### 0.9.x

- [v0.9.6](https://github.com/spotify/scio/releases/tag/v0.9.6) - _"Specialis Revelio"_
- [v0.9.5](https://github.com/spotify/scio/releases/tag/v0.9.5) - _"Colovaria"_
- [v0.9.4](https://github.com/spotify/scio/releases/tag/v0.9.4) - _"Deletrius"_
- [v0.9.3](https://github.com/spotify/scio/releases/tag/v0.9.3) - _"Petrificus Totalus"_
- [v0.9.2](https://github.com/spotify/scio/releases/tag/v0.9.2) - _"Alohomora"_
- [v0.9.1](https://github.com/spotify/scio/releases/tag/v0.9.1) - _"Aberto"_
- [v0.9.0](https://github.com/spotify/scio/releases/tag/v0.9.0) - _"Furnunculus"_

### 0.8.x

- [v0.8.4](https://github.com/spotify/scio/releases/tag/v0.8.4) - _"Expecto Patronum"_
- [v0.8.3](https://github.com/spotify/scio/releases/tag/v0.8.3) - _"Draconifors"_
- [v0.8.2](https://github.com/spotify/scio/releases/tag/v0.8.2) - _"Capacious Extremis"_
- [v0.8.1](https://github.com/spotify/scio/releases/tag/v0.8.1) - _"Bombarda Maxima"_
- [v0.8.0](https://github.com/spotify/scio/releases/tag/v0.8.0) - _"Amato Animo Animato Animagus"_

### 0.7.x

- [v0.7.4](https://github.com/spotify/scio/releases/tag/v0.7.4) - _"Watsonula wautieri"_
- [v0.7.3](https://github.com/spotify/scio/releases/tag/v0.7.3) - _"Vulpes Vulpes"_
- [v0.7.2](https://github.com/spotify/scio/releases/tag/v0.7.2) - _"Ursus t. Ussuricus"_
- [v0.7.1](https://github.com/spotify/scio/releases/tag/v0.7.1) - _"Taxidea Taxus"_
- [v0.7.0](https://github.com/spotify/scio/releases/tag/v0.7.0) - _"Suricata suricatta"_

### 0.6.x

- [v0.6.1](https://github.com/spotify/scio/releases/tag/v0.6.1) - _"Rhyncholestes raphanurus"_
- [v0.6.0](https://github.com/spotify/scio/releases/tag/v0.6.0) - _"Quelea Quelea"_

### 0.5.x

- [v0.5.7](https://github.com/spotify/scio/releases/tag/v0.5.7) - _"Panthera pardus"_
- [v0.5.6](https://github.com/spotify/scio/releases/tag/v0.5.6) - _"Orcinus orca"_
- [v0.5.5](https://github.com/spotify/scio/releases/tag/v0.5.5) - _"Nesolagus netscheri"_
- [v0.5.4](https://github.com/spotify/scio/releases/tag/v0.5.4) - _"Marmota monax"_
- [v0.5.3](https://github.com/spotify/scio/releases/tag/v0.5.3) - _"Lasiorhinus latifrons"_
- [v0.5.2](https://github.com/spotify/scio/releases/tag/v0.5.2) - _"Kobus kob"_
- [v0.5.1](https://github.com/spotify/scio/releases/tag/v0.5.1) - _"Jaculus jerboa"_
- [v0.5.0](https://github.com/spotify/scio/releases/tag/v0.5.0) - _"Ia io"_

### 0.4.x

- [v0.4.7](https://github.com/spotify/scio/releases/tag/v0.4.7) - _"Hydrochoerus hydrochaeris"_
- [v0.4.6](https://github.com/spotify/scio/releases/tag/v0.4.6) - _"Galago gallarum"_
- [v0.4.5](https://github.com/spotify/scio/releases/tag/v0.4.5) - _"Felis ferus"_
- [v0.4.4](https://github.com/spotify/scio/releases/tag/v0.4.4) - _"Erinaceus europaeus"_
- [v0.4.3](https://github.com/spotify/scio/releases/tag/v0.4.3) - _"Dendrohyrax dorsalis"_
- [v0.4.2](https://github.com/spotify/scio/releases/tag/v0.4.2) - _"Castor canadensis"_
- [v0.4.1](https://github.com/spotify/scio/releases/tag/v0.4.1) - _"Blarina brevicauda"_
- [v0.4.0](https://github.com/spotify/scio/releases/tag/v0.4.0) - _"Atelerix albiventris"_

### 0.3.x

- [v0.3.6](https://github.com/spotify/scio/releases/tag/v0.3.6) - _"Veritas odit moras"_
- [v0.3.5](https://github.com/spotify/scio/releases/tag/v0.3.5) - _"Unitas, veritas, carnitas"_
- [v0.3.4](https://github.com/spotify/scio/releases/tag/v0.3.4) - _"Sectumsempra"_
- [v0.3.3](https://github.com/spotify/scio/releases/tag/v0.3.3) - _"Petrificus totalus"_
- [v0.3.2](https://github.com/spotify/scio/releases/tag/v0.3.2) - _"Ut tensio sic vis"_
- [v0.3.1](https://github.com/spotify/scio/releases/tag/v0.3.1) - _"Expecto patronum"_
- [v0.3.0](https://github.com/spotify/scio/releases/tag/v0.3.0) - _"Lux et veritas"_

### 0.2.x

- [v0.2.13](https://github.com/spotify/scio/releases/tag/v0.2.13) - _"Ex luna scientia"_
- [v0.2.12](https://github.com/spotify/scio/releases/tag/v0.2.12) - _"In extremo"_
- [v0.2.11](https://github.com/spotify/scio/releases/tag/v0.2.11) - _"Saltatio mortis"_
- [v0.2.10](https://github.com/spotify/scio/releases/tag/v0.2.10) - _"De Mysteriis Dom Sathanas"_
- [v0.2.9](https://github.com/spotify/scio/releases/tag/v0.2.9) - _"Hoc tempore atque nunc et semper"_
- [v0.2.8](https://github.com/spotify/scio/releases/tag/v0.2.8) - _"Consummatum est"_
- [v0.2.7](https://github.com/spotify/scio/releases/tag/v0.2.7) - _"Crescat scientia vita excolatur"_
- [v0.2.6](https://github.com/spotify/scio/releases/tag/v0.2.6) - _"Sensu lato"_
- [v0.2.5](https://github.com/spotify/scio/releases/tag/v0.2.5) - _"Imperium in imperio"_
- [v0.2.4](https://github.com/spotify/scio/releases/tag/v0.2.4) - _"Ab imo pectore"_
- [v0.2.3](https://github.com/spotify/scio/releases/tag/v0.2.3) - _"Aurea mediocritas"_
- [v0.2.2](https://github.com/spotify/scio/releases/tag/v0.2.2) - _"Intelligenti pauca"_
- [v0.2.1](https://github.com/spotify/scio/releases/tag/v0.2.1) - _"Sedes incertae"_
- [v0.2.0](https://github.com/spotify/scio/releases/tag/v0.2.0) - _"Nulli secundus"_

### 0.1.x

- [v0.1.11](https://github.com/spotify/scio/releases/tag/v0.1.11) - _"In silico"_
- [v0.1.10](https://github.com/spotify/scio/releases/tag/v0.1.10) - _"Memento vivere"_
- [v0.1.9](https://github.com/spotify/scio/releases/tag/v0.1.9) - _"Lucem sequimur"_
- [v0.1.8](https://github.com/spotify/scio/releases/tag/v0.1.8) - _"Nemo saltat sobrius"_
- [v0.1.7](https://github.com/spotify/scio/releases/tag/v0.1.7) - _"Spem gregis"_
- [v0.1.6](https://github.com/spotify/scio/releases/tag/v0.1.6) - _"Sic infit"_
- [v0.1.5](https://github.com/spotify/scio/releases/tag/v0.1.5) - _"Ad astra"_
- [v0.1.4](https://github.com/spotify/scio/releases/tag/v0.1.4) - _"Ad arbitrium"_
- [v0.1.3](https://github.com/spotify/scio/releases/tag/v0.1.3) - _"Ut cognoscant te"_
- [v0.1.2](https://github.com/spotify/scio/releases/tag/v0.1.2) - _"Sapere aude"_
- [v0.1.1](https://github.com/spotify/scio/releases/tag/v0.1.1) - _"Festina lente"_
- [v0.1.0](https://github.com/spotify/scio/releases/tag/v0.1.0) - _"Scio me nihil scire"_
