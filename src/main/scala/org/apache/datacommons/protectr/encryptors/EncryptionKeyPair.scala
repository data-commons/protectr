package org.apache.datacommons.protectr.encryptors

import com.n1analytics.paillier.{PaillierPrivateKey, PaillierPublicKey}

class EncryptionKeyPair(seed:Int) {

  private val paillierPrivateKey: PaillierPrivateKey = PaillierPrivateKey.create(seed)

  def getPrivateKey: PaillierPrivateKey = {
    paillierPrivateKey
  }

  def getPublicKey: PaillierPublicKey = {
    paillierPrivateKey.getPublicKey
  }
}
