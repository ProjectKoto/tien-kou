import teleproto from "teleproto"
import * as tgEntity from "@telegraf/entity"
import type * as tgEntityTypes from "@telegraf/entity/types/types.d.ts"
import { AnyObj } from "./common.mts"



// import * as tgRich from "tg-rich-messages"

export type TgMessageLike = { message: string, entities: AnyObj[] }
type TgEntityCompat = teleproto.Api.TypeMessageEntity & { type: string | undefined }

const processTgEntityCompat = (entity: TgEntityCompat) => {
  switch (entity?.className) {
    case "MessageEntityBold":
      entity.type = "bold"
      break
    case "MessageEntityItalic":
      entity.type = "italic"
      break
    case "MessageEntityUnderline":
      entity.type = "underline"
      break
    case "MessageEntityStrike":
      entity.type = "strikethrough"
      break
    case "MessageEntityCode":
      entity.type = "code"
      break
    case "MessageEntityPre":
      entity.type = "pre"
      break
    case "MessageEntitySpoiler":
      entity.type = "spoiler"
      break
    case "MessageEntityUrl":
      entity.type = "url"
      break
    case "MessageEntityTextUrl":
      entity.type = "text_link"
      break
    case "MessageEntityMentionName":
      entity.type = "text_mention"
      break
    case "MessageEntityBlockquote":
      entity.type = entity.collapsed ? "expandable_blockquote" : "blockquote"
      break
    case "MessageEntityCustomEmoji":
      entity.type = "custom_emoji"
      break
    case "MessageEntityMention":
      entity.type = "mention"
      break
    case "MessageEntityHashtag":
      entity.type = "hashtag"
      break
    case "MessageEntityCashtag":
      entity.type = "cashtag"
      break
    case "MessageEntityBotCommand":
      entity.type = "bot_command"
      break
    case "MessageEntityPhone":
      entity.type = "phone_number"
      break
    case "MessageEntityEmail":
      entity.type = "email"
      break
  }
}


export const tgMessageToHtml = async (tgMessage: TgMessageLike) => {
  const entities = tgMessage.entities || []
  for (const entity of entities) {
    processTgEntityCompat(entity as TgEntityCompat)
  }
  return "<div class=\"tg-white-space-preserve white-space-preserve tg-msg\">" + tgEntity.toHTML({
    text: tgMessage.message,
    entities: entities,
  } as tgEntityTypes.Message) + "</div>"
}

const tgRichMessageToHtmlWorker = async (o: AnyObj) => {
  if (typeof o === 'string') {
    return tgEntity.escapers.HTML(o)
  }

  let result = ''
  
  if (o.text) {
    result += await tgRichMessageToHtmlWorker(o.text)
  }

  if (o.texts) {
    for (const t of o.texts) {
      result += await tgRichMessageToHtmlWorker(t)
    }
  }

  if (o.blocks) {
    for (const b of o.blocks) {
      result += await tgRichMessageToHtmlWorker(b)
    }
  }

  if (o.url) {
    result = `<a href="${tgEntity.escapers.HTML(o.url)}" target="_blank">${result}</a>`
  }

  if (o.className === 'PageBlockParagraph') {
    result = `<p>${result}</p>`
  }

  if (o.className && o.className.startsWith('PageBlockHeading')) {
    result = `<h3>${result}</h3>`
  }
  if (o.className === ('PageBlockBlockquoteBlocks')) {
    result = `<blockquote>${result}</blockquote>`
  }

  return result
}

export const tgRichMessageToHtml = async (tgRichMessage: AnyObj) => {
  return "<div class=\"tg-white-space-preserve white-space-preserve tg-msg tg-rich-msg\">" +
    // tgRich.doc(...((tgRichMessage.blocks as tgRich.BlockContent[]) || [])).toHTML() +
    (await tgRichMessageToHtmlWorker(tgRichMessage)) +
    "</div>"
}

