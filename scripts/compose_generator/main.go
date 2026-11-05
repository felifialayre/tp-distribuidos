// compose_generator arma el docker-compose.yaml a partir de la cantidad de
// replicas de cada nodo (nodes.yaml). Calcula los *_AMOUNT de cada nodo segun
// las replicas de sus vecinos, asi solo hay que cambiar un numero.
//
// Uso: go run . <nodes.yaml> <docker-compose.yaml> (o make compose desde la raiz)
package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
)

// Nodes es la cantidad de replicas de cada nodo
type Nodes struct {
	Clients         int
	Gateway         int
	Filter          int
	ImageDownloader int
	Stopwords       int
	Join            int
	Accumulator     int
	Reducer         int
	Sorter          int
	JoinAverage     int
	AboveAverage    int
}

// fields relaciona cada clave de nodes.yaml con su campo
func (n *Nodes) fields() map[string]*int {
	return map[string]*int{
		"clients":          &n.Clients,
		"gateway":          &n.Gateway,
		"filter":           &n.Filter,
		"image_downloader": &n.ImageDownloader,
		"stopwords":        &n.Stopwords,
		"join":             &n.Join,
		"accumulator":      &n.Accumulator,
		"reducer":          &n.Reducer,
		"sorter":           &n.Sorter,
		"join_average":     &n.JoinAverage,
		"above_average":    &n.AboveAverage,
	}
}

// nodes.yaml tiene una linea "nodo: cantidad" por nodo, los # son comentarios
func loadNodes(path string) (Nodes, error) {
	file, err := os.Open(path)
	if err != nil {
		return Nodes{}, err
	}
	defer file.Close()

	var nodes Nodes
	fields := nodes.fields()
	seen := make(map[string]bool, len(fields))
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(strings.SplitN(scanner.Text(), "#", 2)[0])
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		key = strings.TrimSpace(key)
		field, known := fields[key]
		if !ok || !known {
			return Nodes{}, fmt.Errorf("linea invalida en %s: %q", path, line)
		}
		amount, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || amount < 1 {
			return Nodes{}, fmt.Errorf("%s tiene que ser un numero mayor a 0", key)
		}
		*field = amount
		seen[key] = true
	}
	if err := scanner.Err(); err != nil {
		return Nodes{}, err
	}
	for key := range fields {
		if !seen[key] {
			return Nodes{}, fmt.Errorf("falta la cantidad de %s en %s", key, path)
		}
	}
	return nodes, nil
}

// seq devuelve 0..n-1 para recorrer las replicas en el template
func seq(n int) []int {
	list := make([]int, n)
	for i := range list {
		list[i] = i
	}
	return list
}

// los clientes eligen un gateway al azar de esta lista
func gatewayAddresses(n int) string {
	addresses := make([]string, n)
	for i := range addresses {
		addresses[i] = fmt.Sprintf("gateway_%d:8080", i)
	}
	return strings.Join(addresses, ",")
}

func run() int {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "uso: compose_generator <nodes.yaml> <docker-compose.yaml>")
		return 1
	}
	configPath, outputPath := os.Args[1], os.Args[2]

	nodes, err := loadNodes(configPath)
	if err != nil {
		slog.Error("Leyendo la configuracion", "err", err)
		return 1
	}
	// cada cliente lee de input/client_i, si no existe el cliente falla al arrancar
	for i := range nodes.Clients {
		dir := filepath.Join(filepath.Dir(outputPath), "input", fmt.Sprintf("client_%d", i))
		if _, err := os.Stat(dir); err != nil {
			slog.Warn("No existe el input del cliente", "dir", dir)
		}
	}

	tmpl := template.Must(template.New("compose").Funcs(template.FuncMap{
		"seq":              seq,
		"gatewayAddresses": gatewayAddresses,
	}).Parse(composeTemplate))

	file, err := os.Create(outputPath)
	if err != nil {
		slog.Error("Creando el compose", "err", err)
		return 1
	}
	defer file.Close()
	if err := tmpl.Execute(file, nodes); err != nil {
		slog.Error("Generando el compose", "err", err)
		return 1
	}
	slog.Info("Compose generado", "path", outputPath)
	return 0
}

func main() {
	os.Exit(run())
}
