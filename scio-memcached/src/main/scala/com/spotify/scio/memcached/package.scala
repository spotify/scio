package com.spotify.scio

import com.spotify.scio.memcached.syntax.{SCollectionSyntax, ScioContextSyntax}

package object memcached extends ScioContextSyntax with SCollectionSyntax
