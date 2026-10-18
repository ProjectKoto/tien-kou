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

  // misc/Unreleased/tg/[2026-10]/2026-10-07.md

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
  if (o.className === 'TextEmail' && o.email) {
    result = `<a href="mailto:${tgEntity.escapers.HTML(o.email)}" target="_blank">${result}</a>`
  }

  // todo:
  if (o.className === 'TextMath') {
    result = `<div class="tg-rich-math">${tgEntity.escapers.HTML(result)}</div>`
  }

  if (o.className === 'PageBlockParagraph') {
    result = `<p>${result}</p>`
  }

  if (o.className === 'PageBlockDivider') {
    result = `<hr>`
  }
  if (o.className === 'TextBold') {
    result = `<b>${result}</b>`
  }
  if (o.className === 'TextItalic') {
    result = `<i>${result}</i>`
  }
  if (o.className === 'TextUnderline') {
    result = `<u>${result}</u>`
  }
  if (o.className === 'TextStrike') {
    result = `<s>${result}</s>`
  }
  if (o.className === 'TextSpoiler') {
    result = `<span class="tg-rich-spoiler">${result}</span>`
  }
  if (o.className === 'TextMarked') {
    result = `<span class="tg-rich-marked">${result}</span>`
  }
  if (o.className === 'TextFixed') {
    result = `<code class="tg-rich-fixed">${result}</code>`
  }
  if (o.className === 'PageBlockVideo') {
    result = `<div>[Video ${tgEntity.escapers.HTML(o.videoId)} unsupported]</div>`
  }
  if (o.className === 'PageBlockPhoto') {
    result = `<div>[Photo ${tgEntity.escapers.HTML(o.photoId)} unsupported]</div>`
  }
  if (o.className === 'PageBlockAudio') {
    result = `<div>[Audio ${tgEntity.escapers.HTML(o.audioId)} unsupported]</div>`
  }
  if (o.className === 'PageBlockMap') {
    result = `<div>[Map ${tgEntity.escapers.HTML(JSON.stringify(o))} unsupported]</div>`
  }
  if (o.className === 'PageBlockTable') {
    result = `<table>`
    if (o.rows) {
      for (const t of o.rows) {
        result += await tgRichMessageToHtmlWorker(t)
      }
    }
    result += `</table>`
  }
  if (o.className === 'PageTableRow') {
    result = `<tr>`
    if (o.cells) {
      for (const t of o.cells) {
        result += await tgRichMessageToHtmlWorker(t)
      }
    }
    result += `</tr>`
  }
  if (o.className === 'PageTableCell') {
    result = (o.header ? `<th>` : `<td>`) + result + (o.header ? `</th>` : `</td>`)
  }
  if (o.className === 'Collage') {
    result = `<div>[Collage unsupported]`
    if (o.items) {
      for (const t of o.items) {
        result += await tgRichMessageToHtmlWorker(t)
      }
    }
    result += `</div>`
  }
  if (o.className === 'PageBlockDetails') {
    result = `<details><summary>${await tgRichMessageToHtmlWorker(o.title || '')}</summary>${result}</details>`
  }
  if (o.className === 'PageBlockPreformatted') {
    result = `<div class="tg-rich-language">${tgEntity.escapers.HTML(o.language || '')}</div><pre class="tg-rich-preformatted">${result}</code>`
  }

  if (o.className && o.className.startsWith('PageBlockHeading')) {
    result = `<h3>${result}</h3>`
  }
  if (o.className === ('PageBlockBlockquote')) {
    result = `<blockquote>${result}</blockquote>`
  }
  if (o.className === ('PageListItemText') || o.className === ('PageListOrderedItemText')) {
    if (o.checkbox) {
      if (o.checked) {
        result = `<li>✅${result}</li>`
      } else {
        result = `<li>⬜${result}</li>`
      }
    } else {
      result = `<li>${result}</li>`
    }
  }
  if (o.className === ('PageBlockList')) {
    result = `<ul>${result}</ul>`
  }
  if (o.className === ('PageBlockOrderedList')) {
    result = `<ol>${result}</ol>`
  }
  if (o.className === ('PageBlockBlockquoteBlocks')) {
    result = `<blockquote>${result}</blockquote>`
  }
  if (o.className === ('PageBlockPullquote')) {
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

