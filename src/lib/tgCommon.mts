import tg from "telegram"
import * as tgEntity from "@telegraf/entity"
import type * as tgEntityTypes from "@telegraf/entity/types/types.d.ts"
import { AnyObj } from "./common.mts"

export type TgMessageLike = { message: string, entities: AnyObj[] }
type TgEntityCompat = tg.Api.TypeMessageEntity & { type: string | undefined }

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

