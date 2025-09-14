import { Queue as AsyncQueue } from 'async-await-queue'
import libBigInt from "big-integer"
import { BigInteger as LibraryBigInteger } from "big-integer"
import fs from "fs"
import * as input from "input"
import path from "path"
import randomBigint from 'random-bigint'
import replaceAll from 'string.prototype.replaceall'
import telegram from "telegram"
import tgEvents from "telegram/events/index.js"
import { StoreSession } from "telegram/sessions/index.js"
import { throttle } from 'throttle-debounce'
import '../nodeEnv.mts'

import { AnyObj, datePath, genTimestampString, l, le } from '../lib/common.mts'
import { ensureParentDirExists, ensurePathDirExists, pathExists } from "../lib/nodeCommon.mts"
import { TkContext } from '../serve/serveDef.mts'
import { TgMessageLike, tgMessageToHtml } from '../lib/tgCommon.mts'

export const startTgHoard = async (tkCtx: TkContext, onUpdate: () => Promise<void>) => {
  const tkEnv = tkCtx.e

  // eslint-disable-next-line no-constant-condition
  if (false) {
    return
  }

  if (!tkEnv.TELEGRAM_HOARD_DEST_ROOT_PATH) {
    throw new Error("TELEGRAM_HOARD_DEST_ROOT_PATH missing")
  }
  const tgDest = replaceAll(tkEnv.TELEGRAM_HOARD_DEST_ROOT_PATH, '\\', '/')
  l("tgDest", tgDest)
  const tgMediaDest = replaceAll(path.join(tgDest, "media"), '\\', '/')
  l("tgMediaDest", tgMediaDest)

  if (!tkEnv.TELEGRAM_PUB_HOARD_DEST_ROOT_PATH) {
    throw new Error("TELEGRAM_PUB_HOARD_DEST_ROOT_PATH missing")
  }
  const tgPubDest = replaceAll(tkEnv.TELEGRAM_PUB_HOARD_DEST_ROOT_PATH, '\\', '/')
  l("tgPubDest", tgPubDest)
  const tgPubMediaDest = replaceAll(path.join(tgPubDest, "media"), '\\', '/')
  l("tgPubMediaDest", tgPubMediaDest)

  await ensurePathDirExists(tgMediaDest)
  await ensurePathDirExists(tgPubMediaDest)

  const storeSession = new StoreSession("tg_session")
  const client = new telegram.TelegramClient(storeSession, parseInt(tkEnv.TELEGRAM_API_ID!), tkEnv.TELEGRAM_API_HASH!, {
    // useWSS: false, // Important. Most proxies cannot use SSL.
    useWSS: true,
    proxy: {
      ip: "127.0.0.1", // Proxy host (IP or hostname)
      port: parseInt(tkEnv.TELEGRAM_PROXY_PORT!), // Proxy port
      secret: "", // If used MTProxy then you need to provide a secret (or zeros).
      socksType: 5, // If used Socks you can choose 4 or 5.
      timeout: 2, // Timeout (in seconds) for connection,
    },
  })
  await client.start({
    phoneNumber: async () => await input.text("tg phoneNumber?"),
    password: async () => await input.text("tg password?"),
    phoneCode: async () => await input.text("tg phoneCode?"),
    onError: (err) => console.error(err),
  })
  if (!client.connected) {
    throw new Error("Telegram is not connected, there may be further error info above")
  }
  if (!(await client.checkAuthorization())) {
    throw new Error("Telegram !client.checkAuthorization")
  }
  if (!(await client.isUserAuthorized())) {
    throw new Error("Telegram !client.isUserAuthorized")
  }
  const me = await client.getMe()
  l("You should now be connected to Telegram. current userId=", me.id, " userName=", me.username)
  const meIdStr = me.id.toString()

  const throttledSendHoarded = throttle(5000, async () => {
    const randomId = randomBigint(128)!
    await client.invoke(
      new telegram.Api.messages.SendMessage({
        peer: new telegram.Api.InputPeerSelf(),
        message: "hoarded",
        randomId: randomId as unknown as LibraryBigInteger,
      })
    )
  })

  

  const mediaDownQ = new AsyncQueue(1, 1000)
  const appendFileQ = new AsyncQueue(1, 0)

  // TODO: check if message handler has queue logic, ensure it sequential and has time interval
  async function msgEvent(event: tgEvents.NewMessageEvent) {

    // -- message-scope variables

    const desensitizerCollector: (() => Promise<void>)[] = []

    // -- function declaration

    const processGjSessionPeerInfo = async (peerLike: telegram.Api.TypePeer, originalArgs: AnyObj | undefined, originalArgs2: AnyObj | undefined, originalArgs3: AnyObj | undefined, originalArgs4: AnyObj | undefined) => {
      // (id, hash, username, phone, name, date)
      const peerInfo = await storeSession.getEntityRowsById(telegram.utils.getPeerId(peerLike), true)
      if (peerInfo && Array.isArray(peerInfo) && peerInfo.length >= 6) {
        peerInfo[1] = null
        peerInfo[3] = null
        peerInfo[5] = null
      }
      
      (peerLike as AnyObj).gjSessionPeerInfo = peerInfo
      ;(peerLike.originalArgs as AnyObj).gjSessionPeerInfo = peerInfo
      if (originalArgs) {
        originalArgs.gjSessionPeerInfo = peerInfo
        originalArgs.originalArgs.gjSessionPeerInfo = peerInfo
      }
      if (originalArgs2) {
        originalArgs2.gjSessionPeerInfo = peerInfo
        originalArgs2.originalArgs.gjSessionPeerInfo = peerInfo
      }
      if (originalArgs3) {
        originalArgs3.gjSessionPeerInfo = peerInfo
        originalArgs3.originalArgs.gjSessionPeerInfo = peerInfo
      }
      if (originalArgs4) {
        originalArgs4.gjSessionPeerInfo = peerInfo
        originalArgs4.originalArgs.gjSessionPeerInfo = peerInfo
      }

      const ov = {} as { userId: string, isMe: boolean }
      const doOverwritePeer = () => {
        Object.assign((peerLike as AnyObj), ov)
        if (originalArgs) {
          Object.assign(originalArgs, ov)
          Object.assign(originalArgs.originalArgs, ov)
        }
        if (originalArgs2) {
          Object.assign(originalArgs2, ov)
          Object.assign(originalArgs2.originalArgs, ov)
        }
        if (originalArgs3) {
          Object.assign(originalArgs3, ov)
          Object.assign(originalArgs3.originalArgs, ov)
        }
        if (originalArgs4) {
          Object.assign(originalArgs4, ov)
          Object.assign(originalArgs4.originalArgs, ov)
        }
      }
      if (peerLike.className === "PeerUser" && peerLike.userId.toString() === me.id.toString()) {
        desensitizerCollector.push(async () => {
          peerInfo[0] = "me"
          peerInfo[2] = undefined
          peerInfo[4] = "me"
          ov.userId = "0"
          ov.isMe = true
          doOverwritePeer()
        })
      } else {
        desensitizerCollector.push(async () => {
          ov.isMe = false
          doOverwritePeer()
        })
      }
      
      return peerInfo
    }

    const resolveMessagePeerInplace = async (theMessage: telegram.Api.Message) => {
      if (theMessage.fromId && typeof theMessage.fromId === "object") {
        try {
          await processGjSessionPeerInfo(theMessage.fromId, theMessage.originalArgs.fromId, undefined, undefined, undefined)
        } catch (e) {
          le("resolveMessagePeerInplace: resolve fromId err", e)
        }
      }
      if (theMessage.peerId && typeof theMessage.peerId === "object") {
        try {
          await processGjSessionPeerInfo(theMessage.peerId, theMessage.originalArgs.peerId, undefined, undefined, undefined)
        } catch (e) {
          le("resolveMessagePeerInplace: resolve peerId err", e)
        }
      }
      if (theMessage.fwdFrom && typeof theMessage.fwdFrom === "object" && theMessage.fwdFrom.fromId && typeof theMessage.fwdFrom.fromId === "object") {
        try {
          await processGjSessionPeerInfo(theMessage.fwdFrom.fromId, theMessage.originalArgs.fwdFrom.fromId, theMessage.originalArgs.fwdFrom.originalArgs.fromId, theMessage.fwdFrom.originalArgs.fromId, undefined)
        } catch (e) {
          le("resolveMessagePeerInplace: resolve fwdFrom.fromId err", e)
        }
      }
      if (theMessage.fwdFrom && typeof theMessage.fwdFrom === "object" && theMessage.fwdFrom.savedFromPeer && typeof theMessage.fwdFrom.savedFromPeer === "object") {
        try {
          await processGjSessionPeerInfo(theMessage.fwdFrom.savedFromPeer, theMessage.originalArgs.fwdFrom.savedFromPeer, theMessage.originalArgs.fwdFrom.originalArgs.savedFromPeer, theMessage.fwdFrom.originalArgs.savedFromPeer, undefined)
        } catch (e) {
          le("resolveMessagePeerInplace: resolve fwdFrom.savedFromPeer err", e)
        }
      }
      if (theMessage.fwdFrom && typeof theMessage.fwdFrom === "object" && theMessage.fwdFrom.savedFromId && typeof theMessage.fwdFrom.savedFromId === "object") {
        try {
          await processGjSessionPeerInfo(theMessage.fwdFrom.savedFromId, theMessage.originalArgs.fwdFrom.savedFromId, theMessage.originalArgs.fwdFrom.originalArgs.savedFromId, theMessage.fwdFrom.originalArgs.savedFromId, undefined)
        } catch (e) {
          le("resolveMessagePeerInplace: resolve fwdFrom.savedFromId err", e)
        }
      }

      if ((theMessage as AnyObj).savedPeerId && typeof (theMessage as AnyObj).savedPeerId === "object") {
        try {
          await processGjSessionPeerInfo((theMessage as AnyObj).savedPeerId, theMessage.originalArgs.savedPeerId, undefined, undefined, undefined)
        } catch (e) {
          le("resolveMessagePeerInplace: resolve savedPeerId err", e)
        }
      }
    }

    const resolveExtInvolvedMessage = async (theMessage: telegram.Api.Message, onlyTellExistence: boolean = false) => {
      const isReply = !!theMessage?.replyTo?.replyToMsgId
      const isFwd = !!theMessage?.fwdFrom?.fromId

      if (!isReply && !isFwd) {
        return undefined
      }
      
      let involvementType: string
      if (isReply) {
        involvementType = 'reply'
      } else {
        involvementType = 'fwd'
      }

      if (onlyTellExistence) {
        return {
          involvementType,
          isOnlyExistence: true,
          message: '...'
        } as unknown as telegram.Api.Message
      }

      await new Promise(r => setTimeout(r, 1500))

      if (isReply) {
        // WARN: when current user havn't joined this peer (channel), this FETCHES THE WRONG MESSAGE
        // let repliedMessage = await theMessage?.getReplyMessage()
        let repliedMessage: telegram.Api.Message | undefined = undefined

        if (repliedMessage === undefined) {
          // will cause FLOOD_WAIT. CAUTION!!!
          // await client.getEntity(...)

          l("theMessage?.replyTo?.replyToPeerId", theMessage?.replyTo?.replyToPeerId)
          const inputPeer = await client.getInputEntity(theMessage?.replyTo?.replyToPeerId || theMessage?.peerId as telegram.Api.TypePeer)
          // l("inputPeer", inputPeer)
          // const inputPeer = await client.getInputEntity(new telegram.Api.Channel({
          //   id: (theMessage?.replyTo?.replyToPeerId as telegram.Api.PeerChannel).channelId
          // } as telegram.Api.Channel))

          if (theMessage?.replyTo?.replyToMsgId) {
            const repliedMsgId = theMessage?.replyTo?.replyToMsgId
            let repliedMsgMinId = repliedMsgId - 10
            if (repliedMsgMinId < 0) {
              repliedMsgMinId = 0
            }
            let repliedMsgMaxId = repliedMsgId + 10
            try {
              const repliedMessageSurroundList = (await client.getMessages(inputPeer, {
                minId: repliedMsgMinId,
                maxId: repliedMsgMaxId
              }))

              for (const m of repliedMessageSurroundList) {
                if (m.id === repliedMsgId) {
                  repliedMessage = m
                }
              }

              if (repliedMessage?.groupedId) {
                let sameGroupedMsgMin: telegram.Api.Message | undefined = undefined
                let sameGroupedMsgs: telegram.Api.Message[] = []
                for (const m of repliedMessageSurroundList) {
                  if (m.groupedId && m.groupedId.equals(repliedMessage.groupedId!)) {
                    if (sameGroupedMsgMin === undefined || sameGroupedMsgMin.id > m.id) {
                      sameGroupedMsgMin = m
                    }
                    sameGroupedMsgs.push(m)
                  }
                }
                if (sameGroupedMsgMin) {
                  sameGroupedMsgs.sort((a, b) => {
                    return a.id - b.id
                  })
                  repliedMessage = sameGroupedMsgMin
                  sameGroupedMsgs = sameGroupedMsgs.filter(x => x.id !== repliedMessage?.id)

                  ;(repliedMessage as AnyObj).sameGroupedMessages = sameGroupedMsgs
                  ;(repliedMessage as AnyObj).originalArgs.sameGroupedMessages = sameGroupedMsgs
                }
              }
            } catch (e) {
              le("resolveExtInvolvedMessage#isReply#client.getMessages err", e)
            }
          }
        }

        if (!repliedMessage) {
          return undefined
        }

        try {
          await resolveMessagePeerInplace(repliedMessage)
        } catch (e) {
          le("resolveExtInvolvedMessage#isReply#resolveMessagePeerInplace err", e)
        }

        ;(repliedMessage as AnyObj).involvementType = involvementType
        if ((repliedMessage as AnyObj).originalArgs) {
          (repliedMessage as AnyObj).originalArgs.involvementType = (repliedMessage as AnyObj).involvementType
        }
        return repliedMessage
      } else if (isFwd) {
        let fwdMessage = undefined as telegram.Api.Message | undefined
        
        l("theMessage?.fwdFrom?.fromId", theMessage?.fwdFrom?.fromId)
        let inputPeer
        if (theMessage?.fwdFrom?.fromId) {
          inputPeer = await client.getInputEntity(theMessage?.fwdFrom?.fromId as telegram.Api.TypePeer)
        } else {
          inputPeer = undefined
        }
        // l("inputPeer", inputPeer)
        // const inputPeer = await client.getInputEntity(new telegram.Api.Channel({
        //   id: (theMessage?.fwdFrom?.fromId as telegram.Api.PeerChannel).channelId
        // } as telegram.Api.Channel))

        const origMsgId = theMessage?.fwdFrom?.savedFromMsgId || theMessage?.fwdFrom?.channelPost
        if (origMsgId && inputPeer) {
          try {
            fwdMessage = (await client.getMessages(inputPeer, {
              ids: new telegram.Api.InputMessageID({ id: origMsgId }),
            }))[0]
          } catch (e) {
            le("resolveExtInvolvedMessage#isFwd#client.getMessages err", e)
          }
        }

        if (!fwdMessage) {
          return undefined
        }

        try {
          await resolveMessagePeerInplace(fwdMessage)
        } catch (e) {
          le("resolveExtInvolvedMessage#isFwd#resolveMessagePeerInplace err", e)
        }

        ;(fwdMessage as AnyObj).involvementType = involvementType
        if ((fwdMessage as AnyObj).originalArgs) {
          (fwdMessage as AnyObj).originalArgs.involvementType = (fwdMessage as AnyObj).involvementType
        }
        return fwdMessage
      }
    }
    
    // -- message handling logic

    if ((event?.message?.peerId as telegram.Api.PeerUser)?.userId?.toString() !== meIdStr) {
      return
    }
    if ((event?.message?.message ?? '') === "hoarded") {
      return
    }
    const now = new Date()
    let messageTextForLog = (event?.message?.message ?? '')
    if (messageTextForLog.length >= 64) {
      messageTextForLog = messageTextForLog.substring(0, 61) + "..."
    }
    messageTextForLog = JSON.stringify(messageTextForLog)
    l(`telegram: processing message ${messageTextForLog}`)
    const m0 = {...(event?.message ?? {})}
    const m: (typeof m0) & { client?: AnyObj, extInvolvedMessage?: AnyObj, extInvolvedMessageLv2?: AnyObj, extInvolvedMessageLv3?: AnyObj, extInvolvedMessageLv4?: AnyObj } = m0
    m.client = undefined
    m._client = undefined
    await resolveMessagePeerInplace(m as telegram.Api.Message)

    const message = event?.message
    let extInvolvedMessage: telegram.Api.Message | AnyObj | undefined = undefined
    let extInvolvedMessageLv2: telegram.Api.Message | AnyObj | undefined = undefined
    let extInvolvedMessageLv3: telegram.Api.Message | AnyObj | undefined = undefined
    let extInvolvedMessageLv4: telegram.Api.Message | AnyObj | undefined = undefined

    extInvolvedMessage = await resolveExtInvolvedMessage(event.message)
    if (extInvolvedMessage) {
      m.extInvolvedMessage = {
        ...extInvolvedMessage
      }
      m.extInvolvedMessage!._eventBuilders = undefined
      m.extInvolvedMessage!.client = undefined
      m.extInvolvedMessage!._client = undefined
      // to make gramjs toJSON() happy
      m.originalArgs.extInvolvedMessage = m.extInvolvedMessage
      extInvolvedMessageLv2 = await resolveExtInvolvedMessage(extInvolvedMessage as telegram.Api.Message)
      if (extInvolvedMessageLv2) {
        const mm = {
          ...extInvolvedMessageLv2
        }
        m.extInvolvedMessageLv2 = mm
        m.originalArgs.extInvolvedMessageLv2 = mm
        ;(mm as AnyObj)._eventBuilders = undefined
        ;(mm as AnyObj).client = undefined
        mm._client = undefined

        extInvolvedMessageLv3 = await resolveExtInvolvedMessage(extInvolvedMessageLv2 as telegram.Api.Message)
        if (extInvolvedMessageLv3) {
          const mmm = {
            ...extInvolvedMessageLv3
          }
          m.extInvolvedMessageLv3 = mmm
          m.originalArgs.extInvolvedMessageLv3 = mmm
          ;(mmm as AnyObj)._eventBuilders = undefined
          ;(mmm as AnyObj).client = undefined
          mmm._client = undefined

          extInvolvedMessageLv4 = await resolveExtInvolvedMessage(extInvolvedMessageLv3 as telegram.Api.Message)
          if (extInvolvedMessageLv4) {
            const mmmm = {
              ...extInvolvedMessageLv4
            }
            m.extInvolvedMessageLv4 = mmmm
            m.originalArgs.extInvolvedMessageLv4 = mmmm
            ;(mmmm as AnyObj)._eventBuilders = undefined
            ;(mmmm as AnyObj).client = undefined
            mmmm._client = undefined

            const haveMoreLevelExtInvolvedMessage = await resolveExtInvolvedMessage(extInvolvedMessageLv4 as telegram.Api.Message, true)
            if (haveMoreLevelExtInvolvedMessage) {
              (m as AnyObj).haveMoreLevelExtInvolvedMessage = haveMoreLevelExtInvolvedMessage
              m.originalArgs.haveMoreLevelExtInvolvedMessage = haveMoreLevelExtInvolvedMessage
            }
          }
        }
      }
    }

    let currDest
    let currMediaDest
    let jsonMessageToBeSaved
    if (message.message === "pub" && m.extInvolvedMessage) {
      currDest = tgPubDest
      currMediaDest = tgPubMediaDest
      m.extInvolvedMessage.extInvolvedMessage = m.extInvolvedMessageLv2
      m.extInvolvedMessage.extInvolvedMessageLv2 = m.extInvolvedMessageLv3
      m.extInvolvedMessage.extInvolvedMessageLv3 = m.extInvolvedMessageLv4
      m.extInvolvedMessage.haveMoreLevelExtInvolvedMessage = (m as AnyObj).haveMoreLevelExtInvolvedMessage
      m.extInvolvedMessage.involvementType = undefined

      if (m.extInvolvedMessage.originalArgs) {
        m.extInvolvedMessage.originalArgs.extInvolvedMessage = m.extInvolvedMessage.extInvolvedMessage
        m.extInvolvedMessage.originalArgs.extInvolvedMessageLv2 = m.extInvolvedMessage.extInvolvedMessageLv2
        m.extInvolvedMessage.originalArgs.extInvolvedMessageLv3 = m.extInvolvedMessage.extInvolvedMessageLv3
        m.extInvolvedMessage.originalArgs.involvementType = m.extInvolvedMessage.involvementType
      }

      jsonMessageToBeSaved = m.extInvolvedMessage
    } else {
      currDest = tgDest
      currMediaDest = tgMediaDest
      jsonMessageToBeSaved = m
    }

    for (const d of desensitizerCollector) {
      await d()
    }

    let messagesToFetchMedia = [message, extInvolvedMessage, extInvolvedMessageLv2, extInvolvedMessageLv3, extInvolvedMessageLv4]
    messagesToFetchMedia.push(...messagesToFetchMedia.flatMap(m => (m as AnyObj)?.sameGroupedMessages || []))
    for (const currM of messagesToFetchMedia) {
      if (currM) {
        if (currM.media?.document?.id) {
          if (libBigInt(currM.media?.document?.size).greater(libBigInt(1024*1024*2))) {
            currM.media.document.isUnavailable = true
          } else {
            await mediaDownQ.run(async () => {
              const p = datePath(currMediaDest, currM.media?.document?.id.toString() + ".dat", "[%y-%m]/m-", now)
              await ensureParentDirExists(p)
              await client.downloadMedia(currM as telegram.Api.Message, {
                outputFile: p,
              })
              l(`telegram: saved file in ${p}`)
            })
          }
        }
        if (currM.media?.photo?.id) {
          await mediaDownQ.run(async () => {
            const p = datePath(currMediaDest, currM.media?.photo?.id.toString() + ".png", "[%y-%m]/m-", now)
            await ensureParentDirExists(p)
            await client.downloadMedia(currM as telegram.Api.Message, {
              outputFile: p,
            })
            l(`telegram: saved media photo in ${p}`)
          })
        }
        if (currM.media?.webpage?.photo?.id) {
          await mediaDownQ.run(async () => {
            const p = datePath(currMediaDest, currM.media?.webpage?.photo?.id.toString() + ".png", "[%y-%m]/m-", now)
            await ensureParentDirExists(p)
            await client.downloadMedia(currM as telegram.Api.Message, {
              outputFile: p,
            })
            l(`telegram: saved webpage photo in ${p}`)
          })
        }
        // if (currM.media?.document?.id) {

        // }
      }
    }

    const jsonString = JSON.stringify(jsonMessageToBeSaved, function (key, value) {
      const keyLower = key.toLowerCase()
      if (key === '_client' || key === 'client' || key === 'originalArgs' || keyLower.indexOf("accesshash") !== -1) {
        return undefined
      }
      if (key.length > 0 && key[0] === '_') {
        return undefined
      }
      return value 
    }, 0)

    const tgFilePath = datePath(currDest, ".md", "[%y-%m]/%y-%m-%d", now)

    await appendFileQ.run(async () => {
      await ensureParentDirExists(tgFilePath)

      if (!(await pathExists(tgFilePath))) {
        l("telegram: " + tgFilePath + " not exists, creating")
        await fs.promises.writeFile(tgFilePath, `---\nisDerivableIntoChildren: true\nisTelegramRecord: true\n---\n`, {
          encoding: 'utf-8',
          flush: true,
        })
      }
      await fs.promises.appendFile(tgFilePath, `\n${genTimestampString(now)} tg=${jsonString}\n\n`, {
        encoding: 'utf-8',
        flush: true,
      })
      await fs.promises.appendFile(tgFilePath, `\n${await tgMessageToHtml(jsonMessageToBeSaved as TgMessageLike)}\n`, {
        encoding: 'utf-8',
        flush: true,
      })
    })

    await onUpdate()

    await new Promise(r => setTimeout(r, 300))
    throttledSendHoarded()
    l(`telegram: saved message ${messageTextForLog} in ${tgFilePath}`)
  }

  client.addEventHandler(async function(event: tgEvents.NewMessageEvent) {
    try {
      await msgEvent(event)
    } catch (e) {
      le("telegram hoard message event handler err", e)
      throw e
    }
  }, new tgEvents.NewMessage({}))
}