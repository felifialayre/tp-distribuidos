package stopwords

import (
	"strings"
	"unicode"
)

// palabras vacias de NLTK en ingles y frances (sin las que tienen apostrofe,
// porque tokenize corta ahi y nunca las produce)
// podriamos ver de buscar una libreria o lo que sea
// tmb habria que confirmar si estas son las stopwords que se esperan, en la consigna no dice nada
var stopWords = toSet([]string{
	// ingles
	"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
	"yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself",
	"it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which",
	"who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be",
	"been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an",
	"the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by",
	"for", "with", "about", "against", "between", "into", "through", "during", "before",
	"after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over",
	"under", "again", "further", "then", "once", "here", "there", "when", "where", "why",
	"how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such",
	"no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can",
	"will", "just", "don", "should", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain",
	"aren", "couldn", "didn", "doesn", "hadn", "hasn", "haven", "isn", "ma", "mightn",
	"mustn", "needn", "shan", "shouldn", "wasn", "weren", "won", "wouldn",
	// frances
	"au", "aux", "avec", "ce", "ces", "dans", "de", "des", "du", "elle", "en", "et", "eux",
	"il", "ils", "je", "la", "le", "les", "leur", "lui", "ma", "mais", "me", "même", "mes",
	"moi", "mon", "ne", "nos", "notre", "nous", "on", "ou", "par", "pas", "pour", "qu", "que",
	"qui", "sa", "se", "ses", "son", "sur", "ta", "te", "tes", "toi", "ton", "tu", "un", "une",
	"vos", "votre", "vous", "c", "d", "j", "l", "à", "m", "n", "s", "t", "y", "été", "étée",
	"étées", "étés", "étant", "étante", "étants", "étantes", "suis", "es", "est", "sommes",
	"êtes", "sont", "serai", "seras", "sera", "serons", "serez", "seront", "serais", "serait",
	"serions", "seriez", "seraient", "étais", "était", "étions", "étiez", "étaient", "fus",
	"fut", "fûmes", "fûtes", "furent", "sois", "soit", "soyons", "soyez", "soient", "fusse",
	"fusses", "fût", "fussions", "fussiez", "fussent", "ayant", "ayante", "ayantes", "ayants",
	"eu", "eue", "eues", "eus", "ai", "as", "avons", "avez", "ont", "aurai", "auras", "aura",
	"aurons", "aurez", "auront", "aurais", "aurait", "aurions", "auriez", "auraient", "avais",
	"avait", "avions", "aviez", "avaient", "eut", "eûmes", "eûtes", "eurent", "aie", "aies",
	"ait", "ayons", "ayez", "aient", "eusse", "eusses", "eût", "eussions", "eussiez", "eussent",
})

func toSet(words []string) map[string]struct{} {
	set := make(map[string]struct{}, len(words)) //struct vacio no ocupa memoria
	for _, w := range words {
		set[w] = struct{}{}
	}
	return set
}

// words devuelve las palabras distintas del texto, en minuscula, sin numeros ni stopwords
func words(text string) []string {
	//Usamos este para que el resultado de igual al de la notebook de python
	// o sea simulamos como lo hace python, tal vez ni hace falta

	text = strings.ReplaceAll(text, "İ", "i\u0307")

	// voy cortando el string en pedazos segun si la funcion es true
	tokens := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '_'
	})

	seen := make(map[string]struct{}, len(tokens))
	result := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if isNumber(token) {
			continue
		}
		if _, ok := stopWords[token]; ok {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		result = append(result, token)
	}
	return result
}

func isNumber(token string) bool {
	for _, r := range token {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}
